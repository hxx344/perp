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
import math
import random
import os
import json
import sys
import secrets
import signal
import statistics
import time
from datetime import datetime, timezone
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple, cast
from urllib.parse import quote_plus

try:
    import dotenv  # type: ignore
except Exception:  # pragma: no cover
    dotenv = None

from aiohttp import ClientSession, ClientTimeout, web

import uuid

# Ensure imports work regardless of the current working directory or service wrapper.
# Some deployments end up with sys.path[0] == ".../strategies", which breaks
# imports like `from exchanges...`. We pin the repository root (parent of this
# file's directory) so intra-repo imports resolve.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_repo_dotenv() -> bool:
    """Best-effort load of `perp-dex-tools/.env`.

    - Does NOT override existing process environment variables.
    - Does NOT raise if dotenv isn't installed.
    """

    if dotenv is None:
        return False
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return False
    try:
        return bool(dotenv.load_dotenv(env_path, override=False))
    except Exception:
        return False


# Make coordinator behavior consistent with other scripts that accept an env file.
_load_repo_dotenv()

# Local imports (support both package mode and script mode).
try:
    from .adjustments import AdjustmentAction, GrvtAdjustmentManager  # type: ignore
    from .backpack_adjustments import BackpackAdjustmentManager  # type: ignore
    from .backpack_bookticker_ws import BackpackBookTickerWS  # type: ignore
except ImportError:  # pragma: no cover
    from adjustments import AdjustmentAction, GrvtAdjustmentManager  # type: ignore
    from backpack_adjustments import BackpackAdjustmentManager  # type: ignore
    from backpack_bookticker_ws import BackpackBookTickerWS  # type: ignore

# Optional Backpack trading dependencies.
# Note: exchanges.backpack in this repo exports BackpackClient(config: dict). It
# does not expose TradingConfig (that was from an older SDK wrapper).
_BACKPACK_IMPORT_ERROR: Optional[str] = None
try:
    from exchanges.backpack import BackpackClient  # type: ignore
except Exception as exc:  # pragma: no cover
    BackpackClient = None  # type: ignore
    _BACKPACK_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"
    logging.getLogger("hedge.coordinator").exception(
        "Backpack dependency import failed: %s",
        _BACKPACK_IMPORT_ERROR,
    )


def _make_backpack_client(symbol: str = "") -> Any:
    """Create Backpack client if dependencies are available.

    This is intentionally lazy: when Backpack deps aren't installed, callers should
    gracefully return 500 with a clear message.
    """

    if BackpackClient is None:
        raise RuntimeError("Backpack dependencies not available")

    # Minimal config expected by BaseExchangeClient/BackpackClient implementation.
    sym = str(symbol or "").strip() or "ETH-PERP"
    cfg: Dict[str, Any] = {
        "ticker": sym,
        "contract_id": sym,
    }
    return BackpackClient(cfg)  # type: ignore[misc]


def _bp_env_diagnostics() -> Dict[str, Any]:
    required = ["BACKPACK_PUBLIC_KEY", "BACKPACK_SECRET_KEY"]
    missing = [name for name in required if not (os.getenv(name) or "").strip()]
    return {
        "required": required,
        "missing": missing,
        "configured": len(missing) == 0,
    }

PERSISTED_PARA_AUTO_BALANCE_FILE = Path(__file__).with_name(".para_auto_balance_config.json")
PERSISTED_BP_AUTO_BALANCE_FILE = Path(__file__).with_name(".bp_auto_balance_config.json")
BP_VOLUME_HISTORY_FILE = Path(__file__).with_name(".bp_volume_history.json")
BP_ADJUSTMENTS_HISTORY_FILE = Path(__file__).with_name(".bp_adjustments_history.json")
DASHBOARD_PATH = Path(__file__).with_name("hedge_dashboard.html")
SERVER_PID = os.getpid()
SERVER_INSTANCE_ID = secrets.token_hex(8)


def _load_json_file(path: Path, *, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        raw = path.read_text(encoding="utf-8")
        if not raw.strip():
            return default
        return json.loads(raw)
    except Exception:
        return default


def _atomic_write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

# HistoricalPriceFetcher removed (Binance klines dependency removed).
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


def _coord_debug_enabled() -> bool:
    # Coordinator-wide debug switch (safe to enable in production temporarily).
    return _env_debug("HEDGE_COORD_DEBUG", False)


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        text = str(value).strip()
        if not text:
            return Decimal(default)
        return Decimal(text)
    except Exception:
        return Decimal(default)


def _now_ts() -> float:
    return time.time()


@dataclass
class BackpackVolumeConfig:
    symbol: str = ""
    qty_per_cycle: Decimal = Decimal("0")
    cycles: int = 0  # 0 = run until stopped
    max_spread_bps: float = 0.0
    cooldown_ms: int = 1500
    cooldown_ms_min: int = 0
    cooldown_ms_max: int = 0
    depth_safety_factor: float = 0.7
    min_cap_qty: Decimal = Decimal("0")
    fee_rate: float = 0.00026
    rebate_rate: float = 0.45
    net_fee_rate: float = 0.000143  # fee_rate * (1 - rebate_rate)
    qty_scale: int = 0


@dataclass
class BackpackVolumeCycleRecord:
    ts: float
    symbol: str
    bid1: Decimal
    ask1: Decimal
    bid1_qty: Decimal
    ask1_qty: Decimal
    spread_bps: float
    qty_exec: Decimal
    buy_avg: Decimal
    sell_avg: Decimal
    paired_qty: Decimal
    wear_spread: Decimal
    fee_gross: Decimal
    fee_net: Decimal
    wear_total_net: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "symbol": self.symbol,
            "bid1": str(self.bid1),
            "ask1": str(self.ask1),
            "bid1_qty": str(self.bid1_qty),
            "ask1_qty": str(self.ask1_qty),
            "spread_bps": self.spread_bps,
            "qty_exec": str(self.qty_exec),
            "buy_avg": str(self.buy_avg),
            "sell_avg": str(self.sell_avg),
            "paired_qty": str(self.paired_qty),
            "wear_spread": str(self.wear_spread),
            "fee_gross": str(self.fee_gross),
            "fee_net": str(self.fee_net),
            "wear_total_net": str(self.wear_total_net),
        }


@dataclass
class BackpackVolumeSummary:
    cycles_done: int = 0
    volume_base_total: Decimal = Decimal("0")
    volume_quote_total: Decimal = Decimal("0")
    wear_spread_total: Decimal = Decimal("0")
    fee_total_gross: Decimal = Decimal("0")
    fee_total_net: Decimal = Decimal("0")
    rebate_total: Decimal = Decimal("0")

    @staticmethod
    def from_snapshot(payload: Any) -> "BackpackVolumeSummary":
        """Best-effort load persisted summary dict (strings) into a BackpackVolumeSummary."""
        if not isinstance(payload, dict):
            return BackpackVolumeSummary()

        def _d(key: str) -> Decimal:
            try:
                return Decimal(str(payload.get(key) or "0"))
            except Exception:
                return Decimal("0")

        out = BackpackVolumeSummary()
        try:
            out.cycles_done = int(payload.get("cycles_done") or 0)
        except Exception:
            out.cycles_done = 0
        out.volume_base_total = _d("volume_base_total")
        out.volume_quote_total = _d("volume_quote_total")
        out.wear_spread_total = _d("wear_spread_total")
        out.fee_total_gross = _d("fee_total_gross")
        out.fee_total_net = _d("fee_total_net")
        out.rebate_total = _d("rebate_total")
        return out

    def to_dict(self, cfg: BackpackVolumeConfig) -> Dict[str, Any]:
        wear_total_net = self.wear_spread_total + self.fee_total_net
        wear_per10k_net = None
        try:
            if self.volume_quote_total > 0:
                wear_per10k_net = float((wear_total_net / self.volume_quote_total) * Decimal("10000"))
        except Exception:
            wear_per10k_net = None

        return {
            "cycles_done": self.cycles_done,
            "volume_base_total": str(self.volume_base_total),
            "volume_quote_total": str(self.volume_quote_total),
            "wear_spread_total": str(self.wear_spread_total),
            "fee_rate": cfg.fee_rate,
            "rebate_rate": cfg.rebate_rate,
            "fee_total_gross": str(self.fee_total_gross),
            "fee_total_net": str(self.fee_total_net),
            "rebate_total": str(self.rebate_total),
            "wear_total_net": str(wear_total_net),
            "wear_per10k_net": wear_per10k_net,
        }


@dataclass
class BackpackVolumeState:
    running: bool = False
    run_id: str = ""
    started_at: Optional[float] = None
    last_error: str = ""
    # Non-error informational message shown in status (e.g. waiting for depth)
    last_note: str = ""
    # Runner diagnostics (helps debug "running but no fills" from the panel)
    last_gate: str = ""
    # Best-effort runner liveness hints for status/debug.
    last_heartbeat_ts: float = 0.0
    last_cycle_attempt_ts: float = 0.0
    task_last_exception: str = ""
    last_bid1: str = ""
    last_ask1: str = ""
    last_cap_qty: str = ""
    last_spread_bps: Optional[float] = None
    last_qty_exec: str = ""
    cfg: BackpackVolumeConfig = field(default_factory=BackpackVolumeConfig)
    summary: BackpackVolumeSummary = field(default_factory=BackpackVolumeSummary)
    recent: Deque[BackpackVolumeCycleRecord] = field(default_factory=lambda: deque(maxlen=200))
    task: Optional[asyncio.Task] = None


@dataclass
class BackpackVolumeMultiState:
    """Manage multiple concurrent Backpack volume runners keyed by symbol."""

    states: Dict[str, BackpackVolumeState] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)



def _env_debug(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        text = str(raw).strip().lower()
    except Exception:
        return default
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


PARA_STALE_DEBUG_ENV = "PARA_STALE_DEBUG"
PARA_RISK_DEBUG_ENV = "PARA_RISK_DEBUG"
GLOBAL_RISK_ALERT_KEY = "__global_risk__"
PARA_RISK_ALERT_KEY = "__para_risk__"
BP_RISK_ALERT_KEY = "__bp_risk__"
TRANSFERABLE_HISTORY_LIMIT = 720
TRANSFERABLE_HISTORY_MERGE_SECONDS = 20.0
ALERT_HISTORY_LIMIT = 200

# Feishu webhook push (PARA risk snapshot)
FEISHU_WEBHOOK_ENV = "FEISHU_WEBHOOK_URL"
FEISHU_PARA_PUSH_ENABLED_ENV = "FEISHU_PARA_PUSH_ENABLED"
FEISHU_PARA_PUSH_INTERVAL_ENV = "FEISHU_PARA_PUSH_INTERVAL"

# Feishu webhook push (Backpack snapshot)
FEISHU_BP_PUSH_ENABLED_ENV = "FEISHU_BP_PUSH_ENABLED"
FEISHU_BP_PUSH_INTERVAL_ENV = "FEISHU_BP_PUSH_INTERVAL"

# Keep the buffer logic aligned with hedge_dashboard.html
RISK_CAPACITY_DEVIATION_THRESHOLD = 0.12
RISK_CAPACITY_PENDING_TOLERANCE = 0.10
RISK_CAPACITY_CONFIRMATION_CYCLES = 3
RISK_CAPACITY_MIN_ABS_DELTA = 100


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = raw.strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _format_percent(value: float, decimals: int = 2) -> str:
    if value is None or not math.isfinite(value):
        return "-"
    return f"{value:.{max(int(decimals), 0)}f}%"


def _format_decimal(value: Decimal, decimals: int = 2) -> str:
    try:
        quant = Decimal("1") if decimals <= 0 else Decimal("1").scaleb(-decimals)
        return format(value.quantize(quant), "f")
    except Exception:
        return str(value)


@dataclass
class RiskCapacityBufferState:
    accepted_value: Optional[Decimal] = None
    accepted_at: Optional[float] = None
    pending_value: Optional[Decimal] = None
    pending_cycles: int = 0
    pending_delta_pct: float = 0.0
    pending_since: Optional[float] = None


def _evaluate_risk_capacity_buffer(
    *,
    has_value: bool,
    value: Optional[Decimal],
    timestamp: Optional[float],
    base_note: str,
    state: RiskCapacityBufferState,
) -> Tuple[Optional[Decimal], str, str]:
    """Python port of `evaluateRiskCapacityBuffer` in hedge_dashboard.html."""

    safe_timestamp = float(timestamp) if timestamp is not None and math.isfinite(float(timestamp)) else time.time()
    safe_base_note = base_note or ""
    if not has_value or value is None:
        state.pending_value = None
        state.pending_cycles = 0
        state.pending_delta_pct = 0.0
        state.pending_since = None
        if state.accepted_value is None:
            return None, (safe_base_note or "缺少风险基数数据"), "missing"
        note_parts = [part for part in (safe_base_note, "沿用上次值") if part]
        return state.accepted_value, " · ".join(note_parts), "stale"

    if state.accepted_value is None:
        state.accepted_value = value
        state.accepted_at = safe_timestamp
        state.pending_value = None
        state.pending_cycles = 0
        state.pending_delta_pct = 0.0
        state.pending_since = None
        return value, safe_base_note, "fresh"

    previous = state.accepted_value
    delta = abs(value - previous)
    base = max(abs(previous), Decimal("1"))
    try:
        percent_delta = float(delta / base)
    except Exception:
        percent_delta = 0.0
    exceeds_threshold = percent_delta >= RISK_CAPACITY_DEVIATION_THRESHOLD and delta >= Decimal(str(RISK_CAPACITY_MIN_ABS_DELTA))

    if not exceeds_threshold:
        state.accepted_value = value
        state.accepted_at = safe_timestamp
        state.pending_value = None
        state.pending_cycles = 0
        state.pending_delta_pct = 0.0
        state.pending_since = None
        return value, safe_base_note, "fresh"

    tolerance = max(abs(value), Decimal("1")) * Decimal(str(RISK_CAPACITY_PENDING_TOLERANCE))
    if state.pending_value is not None and abs(state.pending_value - value) <= tolerance:
        state.pending_cycles += 1
    else:
        state.pending_value = value
        state.pending_cycles = 1
        state.pending_since = safe_timestamp
    state.pending_delta_pct = percent_delta

    if state.pending_cycles >= RISK_CAPACITY_CONFIRMATION_CYCLES:
        state.accepted_value = value
        state.accepted_at = safe_timestamp
        state.pending_value = None
        state.pending_cycles = 0
        state.pending_delta_pct = 0.0
        state.pending_since = None
        return value, safe_base_note, "fresh"

    note_parts = [part for part in (safe_base_note,) if part]
    note_parts.append(f"数据确认中 ({state.pending_cycles}/{RISK_CAPACITY_CONFIRMATION_CYCLES})")
    note_parts.append(f"偏差 {_format_percent(percent_delta * 100, 1)}")
    return previous, " · ".join(note_parts), "pending"

DEFAULT_MARGIN_SCHEDULE: Tuple[Tuple[str, str, str], ...] = (
    ("600000", "0.02", "0.01"),
    ("1600000", "0.04", "0.02"),
    ("4000000", "0.05", "0.025"),
    ("10000000", "0.1", "0.05"),
    ("20000000", "0.2", "0.1"),
    ("50000000", "0.25", "0.125"),
    ("80000000", "0.3333", "0.1667"),
    ("101000000", "0.5", "0.25"),
    ("Infinity", "1", "0.5"),
)

MARGIN_SCHEDULES: Dict[str, Tuple[Tuple[str, str, str], ...]] = {
    "BTC": DEFAULT_MARGIN_SCHEDULE,
    "ETH": DEFAULT_MARGIN_SCHEDULE,
}


class BarkNotifier:
    def __init__(self, url_template: str, timeout: float = 10.0, append_payload: bool = True) -> None:
        self._template = (url_template or "").strip()
        self._timeout = max(timeout, 1.0)
        # append_payload=False -> 纯 URL 模式，不拼接/替换标题正文
        self._append_payload = bool(append_payload)

    async def send(self, *, title: str = "", body: str = "") -> None:
        if not self._template:
            LOGGER.warning("Bark notifier missing push URL; skipping alert")
            return

        if not self._append_payload:
            endpoint = self._template
        else:
            safe_title = quote_plus(title or "Alert")
            safe_body = quote_plus(body or "")
            if "{title}" in self._template or "{body}" in self._template:
                endpoint = self._template.replace("{title}", safe_title).replace("{body}", safe_body)
            else:
                endpoint = f"{self._template.rstrip('/')}/{safe_title}/{safe_body}"

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
class GlobalRiskSnapshot:
    ratio: Optional[float]
    total_transferable: Decimal
    worst_loss_value: Decimal
    worst_agent_id: Optional[str]
    worst_account_label: Optional[str]


@dataclass(frozen=True)
class ParaRiskSnapshot:
    ratio: Optional[float]
    risk_capacity: Decimal
    worst_loss_value: Decimal
    worst_agent_id: Optional[str]
    worst_account_label: Optional[str]
    # Debug/forensics fields: inputs used to compute `risk_capacity`.
    equity_sum: Optional[Decimal] = None
    max_initial_margin: Optional[Decimal] = None
    account_count: Optional[int] = None
    computed_at: Optional[float] = None


@dataclass
class RiskAlertSettings:
    threshold: Optional[float] = None
    reset_ratio: Optional[float] = None
    cooldown: float = 900.0
    bark_url: Optional[str] = None
    bark_append_payload: bool = False  # 仅使用 URL，不附带标题正文
    bark_timeout: float = 10.0
    title_template: str = "Global Risk {ratio_percent:.1f}%"
    body_template: str = (
        "{account_label} ({agent_id}) loss {loss_value} / {base_label} {base_value}"
    )

    def normalized(self) -> "RiskAlertSettings":
        def _clean_ratio(value: Optional[float]) -> Optional[float]:
            if value is None:
                return None
            try:
                ratio = float(value)
            except (TypeError, ValueError):
                return None
            if ratio > 1.0:
                ratio = ratio / 100.0
            if ratio <= 0 or not math.isfinite(ratio):
                return None
            return min(max(ratio, 0.0), 1.0)

        threshold = _clean_ratio(self.threshold)
        reset_ratio = _clean_ratio(self.reset_ratio)
        if threshold is None:
            reset_ratio = None
        elif reset_ratio is None:
            reset_ratio = threshold * 0.7
        else:
            reset_ratio = min(reset_ratio, threshold)

        cooldown = max(float(self.cooldown or 0), 0.0)
        timeout = max(float(self.bark_timeout or 0), 1.0)
        url = (self.bark_url or "").strip() or None
        title = (self.title_template or "Global Risk {ratio_percent:.1f}%").strip()
        body = (
            self.body_template
            or "{account_label} ({agent_id}) loss {loss_value} / {base_label} {base_value}"
        ).strip()
        return RiskAlertSettings(
            threshold=threshold,
            reset_ratio=reset_ratio,
            cooldown=cooldown,
            bark_url=url,
            bark_append_payload=bool(self.bark_append_payload),
            bark_timeout=timeout,
            title_template=title,
            body_template=body,
        )

    def to_payload(self) -> Dict[str, Any]:
        normalized = self.normalized()
        threshold_percent = (
            normalized.threshold * 100 if normalized.threshold is not None else None
        )
        reset_percent = (
            normalized.reset_ratio * 100 if normalized.reset_ratio is not None else None
        )
        return {
            "enabled": bool(normalized.threshold),
            "threshold": normalized.threshold,
            "threshold_percent": threshold_percent,
            "reset_ratio": normalized.reset_ratio,
            "reset_ratio_percent": reset_percent,
            "cooldown": normalized.cooldown,
            "bark_url": normalized.bark_url,
            "bark_append_payload": normalized.bark_append_payload,
            "bark_timeout": normalized.bark_timeout,
            "title_template": normalized.title_template,
            "body_template": normalized.body_template,
        }


@dataclass
class DualRiskAlertSettings:
    global_risk: RiskAlertSettings
    para_risk: RiskAlertSettings

    def to_payload(self) -> Dict[str, Any]:
        return {
            "global_risk": self.global_risk.to_payload(),
            "para_risk": self.para_risk.to_payload(),
        }


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


class HistoricalPriceFetcher:
    _ENDPOINT = "https://api.binance.com/api/v3/klines"
    _MAX_LIMIT = 1000
    _MAX_POINTS = 50000
    _DEFAULT_INTERVAL = "1h"
    _INTERVAL_MINUTES: Dict[str, int] = {
        "1m": 1,
        "3m": 3,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "6h": 360,
        "8h": 480,
        "12h": 720,
        "1d": 1440,
    }

    def __init__(self, *, timeout: float = 10.0, cache_ttl: float = 900.0) -> None:
        self._timeout = max(float(timeout), 3.0)
        self._cache_ttl = max(float(cache_ttl), 60.0)
        self._session: Optional[ClientSession] = None
        self._lock = asyncio.Lock()
        self._cache: Dict[str, Tuple[float, List[Dict[str, float]]]] = {}

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    def interval_minutes(self, interval: Optional[str]) -> int:
        normalized = self._normalize_interval(interval)
        return self._INTERVAL_MINUTES.get(normalized, self._INTERVAL_MINUTES[self._DEFAULT_INTERVAL])

    def normalize_interval(self, interval: Optional[str]) -> str:
        return self._normalize_interval(interval)

    async def get_series(self, symbol: str, *, interval: Optional[str] = None, points: int = 1000) -> List[Dict[str, float]]:
        normalized_symbol = self._normalize_symbol(symbol)
        if not normalized_symbol:
            raise ValueError("symbol is required")
        normalized_interval = self._normalize_interval(interval)
        clamped_points = self._clamp_points(points)
        cache_key = self._cache_key(normalized_symbol, normalized_interval, clamped_points)
        now = time.time()
        async with self._lock:
            cached = self._cache.get(cache_key)
            if cached and cached[0] > now:
                return copy.deepcopy(cached[1])

        series = await self._download_series(normalized_symbol, normalized_interval, clamped_points)

        snapshot = copy.deepcopy(series)
        async with self._lock:
            self._cache[cache_key] = (time.time() + self._cache_ttl, snapshot)
        return series

    def _cache_key(self, symbol: str, interval: str, points: int) -> str:
        return f"{symbol}:{interval}:{points}"

    def _normalize_symbol(self, symbol: Optional[str]) -> str:
        if symbol is None:
            return ""
        try:
            text = str(symbol).strip().upper()
        except Exception:
            return ""
        cleaned = "".join(ch for ch in text if ch.isalnum())
        return cleaned[:50]

    def _normalize_interval(self, interval: Optional[str]) -> str:
        if not interval:
            return self._DEFAULT_INTERVAL
        text = str(interval).strip().lower()
        if text.endswith("m") or text.endswith("h") or text.endswith("d"):
            normalized = text
        else:
            normalized = text + ("m" if text.isdigit() else "")
        normalized = normalized.replace("minutes", "m").replace("hours", "h").replace("days", "d")
        normalized = normalized.replace("mins", "m").replace("hrs", "h")
        normalized = normalized.replace("minute", "m").replace("hour", "h").replace("day", "d")
        normalized = normalized.replace("\u5206", "m")
        normalized = normalized.replace("\u5c0f\u65f6", "h")
        if normalized not in self._INTERVAL_MINUTES:
            return self._DEFAULT_INTERVAL
        return normalized

    def _clamp_points(self, points: int) -> int:
        try:
            value = int(points)
        except (TypeError, ValueError):
            value = 1000
        if value <= 0:
            value = 100
        return max(100, min(value, self._MAX_POINTS))

    def _ensure_session(self) -> ClientSession:
        if self._session is None or self._session.closed:
            self._session = ClientSession(timeout=ClientTimeout(total=self._timeout))
        return self._session

    async def _download_series(self, symbol: str, interval: str, points: int) -> List[Dict[str, float]]:
        session = self._ensure_session()
        collected: List[Dict[str, float]] = []
        end_time_ms: Optional[int] = None

        while len(collected) < points:
            batch_size = min(self._MAX_LIMIT, points - len(collected))
            params: Dict[str, Any] = {
                "symbol": symbol,
                "interval": interval,
                "limit": str(batch_size),
            }
            if end_time_ms is not None:
                params["endTime"] = end_time_ms
            async with session.get(self._ENDPOINT, params=params) as response:
                if response.status != 200:
                    body_preview = (await response.text())[:200]
                    raise RuntimeError(f"HTTP {response.status} {body_preview}")
                payload = await response.json()
            if not isinstance(payload, list) or not payload:
                break
            chunk: List[Dict[str, float]] = []
            for entry in payload:
                try:
                    open_time = float(entry[0]) / 1000.0
                    close_time = float(entry[6]) / 1000.0
                    chunk.append(
                        {
                            "open_time": open_time,
                            "close_time": close_time,
                            "open": float(entry[1]),
                            "high": float(entry[2]),
                            "low": float(entry[3]),
                            "close": float(entry[4]),
                            "volume": float(entry[5]),
                        }
                    )
                except (IndexError, TypeError, ValueError):
                    continue
            if not chunk:
                break
            collected = chunk + collected
            try:
                end_time_ms = int(payload[0][0]) - 1
            except (TypeError, ValueError):
                end_time_ms = None
            if len(payload) < batch_size:
                break

        if not collected:
            return []
        if len(collected) > points:
            collected = collected[-points:]
        series: List[Dict[str, float]] = []
        for entry in collected:
            ts = entry.get("close_time")
            close_price = entry.get("close")
            if isinstance(ts, (int, float)) and isinstance(close_price, (int, float)):
                series.append({
                    "ts": ts,
                    "close": close_price,
                })
        return series


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
    grvt_accounts: Optional[Dict[str, Any]] = None
    paradex_accounts: Optional[Dict[str, Any]] = None
    backpack_accounts: Optional[Dict[str, Any]] = None

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
        grvt_accounts_raw = payload.get("grvt_accounts")
        paradex_accounts_raw = payload.get("paradex_accounts")
        backpack_accounts_raw = payload.get("backpack_accounts")

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

        if isinstance(grvt_accounts_raw, dict):
            normalized_grvt = copy.deepcopy(grvt_accounts_raw)
            accounts_block = normalized_grvt.get("accounts")
            if isinstance(accounts_block, list) and len(accounts_block) > 50:
                normalized_grvt["accounts"] = accounts_block[:50]
            self.grvt_accounts = normalized_grvt

        if isinstance(paradex_accounts_raw, dict):
            normalized_para = copy.deepcopy(paradex_accounts_raw)
            accounts_block = normalized_para.get("accounts")
            if isinstance(accounts_block, list) and len(accounts_block) > 50:
                normalized_para["accounts"] = accounts_block[:50]
            self.paradex_accounts = normalized_para

        if isinstance(backpack_accounts_raw, dict):
            normalized_bp = copy.deepcopy(backpack_accounts_raw)
            accounts_block = normalized_bp.get("accounts")
            if isinstance(accounts_block, list) and len(accounts_block) > 50:
                normalized_bp["accounts"] = accounts_block[:50]
            positions_block = normalized_bp.get("positions")
            if isinstance(positions_block, list) and len(positions_block) > 100:
                normalized_bp["positions"] = positions_block[:100]
            self.backpack_accounts = normalized_bp

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
        if self.grvt_accounts is not None:
            payload["grvt_accounts"] = copy.deepcopy(self.grvt_accounts)
        if self.paradex_accounts is not None:
            payload["paradex_accounts"] = copy.deepcopy(self.paradex_accounts)
        if self.backpack_accounts is not None:
            payload["backpack_accounts"] = copy.deepcopy(self.backpack_accounts)
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
        alert_settings: Optional[RiskAlertSettings] = None,
    ) -> None:
        self._states: Dict[str, HedgeState] = {}
        self._lock = asyncio.Lock()
        self._eviction_seconds = 6 * 3600  # prune entries idle for 6 hours
        self._stale_warning_seconds = 5 * 60  # tag agents as stale after 5 minutes
        self._last_agent_id: Optional[str] = None
        self._transferable_history: Deque[tuple[float, Decimal]] = deque(maxlen=TRANSFERABLE_HISTORY_LIMIT)
        self._controls: Dict[str, Dict[str, Any]] = {}
        self._default_paused: bool = False
        self._alert_settings = (alert_settings or RiskAlertSettings()).normalized()
        self._alert_settings_updated_at = time.time()
        self._para_alert_settings = RiskAlertSettings(
            title_template="PARA Risk {ratio_percent:.1f}%",
            body_template="{account_label} ({agent_id}) loss {loss_value} / {base_label} {base_value}",
        ).normalized()
        self._para_alert_settings_updated_at = time.time()
        self._bp_alert_settings = RiskAlertSettings(
            title_template="Backpack Risk {ratio_percent:.1f}%",
            body_template="{account_label} ({agent_id}) loss {loss_value} / {base_label} {base_value}",
        ).normalized()
        self._bp_alert_settings_updated_at = time.time()
        self._bark_notifier: Optional[BarkNotifier] = None
        self._para_bark_notifier: Optional[BarkNotifier] = None
        self._bp_bark_notifier: Optional[BarkNotifier] = None
        self._risk_alert_threshold: Optional[float] = None
        self._risk_alert_reset: Optional[float] = None
        self._risk_alert_cooldown: float = 0.0

        self._para_risk_alert_threshold: Optional[float] = None
        self._para_risk_alert_reset: Optional[float] = None
        self._para_risk_alert_cooldown: float = 0.0

        self._bp_risk_alert_threshold: Optional[float] = None
        self._bp_risk_alert_reset: Optional[float] = None
        self._bp_risk_alert_cooldown: float = 0.0

        self._para_stale_critical_seconds: float = 30.0
        self._apply_alert_settings()
        self._apply_para_alert_settings()
        self._apply_bp_alert_settings()
        self._risk_alert_active: Dict[str, bool] = {}
        self._risk_alert_last_ts: Dict[str, float] = {}

        self._para_risk_stats: Optional[ParaRiskSnapshot] = None
        self._global_risk_stats: Optional[GlobalRiskSnapshot] = None
        self._alert_history: Deque[Dict[str, Any]] = deque(maxlen=ALERT_HISTORY_LIMIT)
        self._alert_history_lock = asyncio.Lock()

        # Feishu PARA periodic push
        self._feishu_webhook_url = (os.getenv(FEISHU_WEBHOOK_ENV) or "").strip() or None
        self._feishu_para_push_enabled = _env_bool(FEISHU_PARA_PUSH_ENABLED_ENV, False)
        self._feishu_para_push_interval = max(_env_float(FEISHU_PARA_PUSH_INTERVAL_ENV, 300.0), 30.0)
        self._feishu_task: Optional[asyncio.Task[Any]] = None

        # Feishu Backpack periodic push
        self._feishu_bp_push_enabled = _env_bool(FEISHU_BP_PUSH_ENABLED_ENV, False)
        self._feishu_bp_push_interval = max(_env_float(FEISHU_BP_PUSH_INTERVAL_ENV, 300.0), 30.0)
        self._feishu_bp_task: Optional[asyncio.Task[Any]] = None

        self._feishu_session: Optional[ClientSession] = None
        self._feishu_start_diagnostic_logged = False
        self._para_risk_capacity_buffer = RiskCapacityBufferState()
        self._bp_risk_capacity_buffer = RiskCapacityBufferState()

    async def start_background_tasks(self) -> None:
        if not self._feishu_start_diagnostic_logged:
            self._feishu_start_diagnostic_logged = True
            # Log once so operators can immediately tell why Feishu push is (not) active.
            LOGGER.info(
                "Feishu PARA push config: enabled=%s interval=%.0fs url=%s (set %s=1 to enable access logs)",
                self._feishu_para_push_enabled,
                self._feishu_para_push_interval,
                "set" if self._feishu_webhook_url else "missing",
                "AIOHTTP_ACCESS_LOG",
            )
            LOGGER.info(
                "Feishu Backpack push config: enabled=%s interval=%.0fs url=%s",
                self._feishu_bp_push_enabled,
                self._feishu_bp_push_interval,
                "set" if self._feishu_webhook_url else "missing",
            )

        if not self._feishu_webhook_url:
            if self._feishu_para_push_enabled or self._feishu_bp_push_enabled:
                LOGGER.warning(
                    "Feishu push enabled but %s is missing/blank; push task will NOT start",
                    FEISHU_WEBHOOK_ENV,
                )
            return

        # Start shared session used by both tasks.
        if self._feishu_session is None or self._feishu_session.closed:
            self._feishu_session = ClientSession(timeout=ClientTimeout(total=10.0))

        # PARA push task.
        if self._feishu_para_push_enabled and self._feishu_task is None:
            self._feishu_task = asyncio.create_task(self._run_feishu_para_push_loop())
            LOGGER.info(
                "Feishu PARA push enabled: interval=%.0fs url=%s",
                self._feishu_para_push_interval,
                "set" if self._feishu_webhook_url else "missing",
            )
        elif self._feishu_webhook_url and not self._feishu_para_push_enabled:
            LOGGER.info(
                "Feishu PARA push is disabled (%s=%s); webhook is configured but push task will NOT start",
                FEISHU_PARA_PUSH_ENABLED_ENV,
                os.getenv(FEISHU_PARA_PUSH_ENABLED_ENV),
            )

        # Backpack push task.
        if self._feishu_bp_push_enabled and self._feishu_bp_task is None:
            self._feishu_bp_task = asyncio.create_task(self._run_feishu_bp_push_loop())
            LOGGER.info(
                "Feishu Backpack push enabled: interval=%.0fs url=%s",
                self._feishu_bp_push_interval,
                "set" if self._feishu_webhook_url else "missing",
            )
        elif self._feishu_webhook_url and not self._feishu_bp_push_enabled:
            LOGGER.info(
                "Feishu Backpack push is disabled (%s=%s); webhook is configured but push task will NOT start",
                FEISHU_BP_PUSH_ENABLED_ENV,
                os.getenv(FEISHU_BP_PUSH_ENABLED_ENV),
            )

    async def stop_background_tasks(self) -> None:
        task = self._feishu_task
        self._feishu_task = None
        bp_task = self._feishu_bp_task
        self._feishu_bp_task = None
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if bp_task:
            bp_task.cancel()
            with suppress(asyncio.CancelledError):
                await bp_task
        if self._feishu_session and not self._feishu_session.closed:
            await self._feishu_session.close()
        self._feishu_session = None

    async def _run_feishu_para_push_loop(self) -> None:
        try:
            # fire immediately, then every interval
            while True:
                await self._try_send_feishu_para_snapshot()
                await asyncio.sleep(self._feishu_para_push_interval)
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Feishu PARA push loop stopped: %s", exc)

    async def _run_feishu_bp_push_loop(self) -> None:
        try:
            while True:
                await self._try_send_feishu_bp_snapshot()
                await asyncio.sleep(self._feishu_bp_push_interval)
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Feishu Backpack push loop stopped: %s", exc)

    def _build_para_risk_push_text(self, now: Optional[float] = None) -> Optional[str]:
        stats = self._para_risk_stats
        if stats is None:
            return None

        # Keep Feishu push aligned with PARA alert history (frontend-authoritative fields).
        authority = self._compute_para_authority_values()

        ts = now if now is not None else time.time()
        buffered_capacity: Decimal = authority.get("risk_capacity_buffered") or Decimal("0")
        worst_loss: Decimal = authority.get("worst_loss") or Decimal("0")
        ratio = authority.get("ratio")
        capacity_note = authority.get("buffer_note")
        capacity_status = authority.get("buffer_status")

        ratio_text = _format_percent((ratio or 0.0) * 100, 2) if ratio is not None else "-"
        authority_inputs = authority.get("inputs") if isinstance(authority, dict) else None
        if not isinstance(authority_inputs, dict):
            authority_inputs = {}
        worst_label = authority_inputs.get("worst_account_label") or stats.worst_account_label or "-"
        worst_agent = authority_inputs.get("worst_agent_id") or stats.worst_agent_id or "-"

        lines = [
            "[PARA] 风险播报",
            f"RISK LEVEL: {ratio_text}",
            f"风险裕量(risk_capacity): {_format_decimal(buffered_capacity or Decimal('0'), 2)}",
            f"最坏亏损(worst_loss): {_format_decimal(worst_loss, 2)}",
            f"最坏账户: {worst_label} ({worst_agent})",
        ]
        if capacity_note:
            lines.append(f"备注: {capacity_note} ({capacity_status})")
        return "\n".join(lines)

    @staticmethod
    def _feishu_risk_color(ratio: Optional[float]) -> str:
        """Return a Feishu card color token based on risk level.

        We keep it intentionally simple:
        - green: < 20%
        - yellow: 20% - 30%
        - red: >= 30%

        Feishu interactive cards accept tokens like: green / orange / red / grey.
        """

        if ratio is None or not isinstance(ratio, (int, float)) or not math.isfinite(float(ratio)):
            return "grey"
        value = float(ratio)
        if value >= 0.30:
            return "red"
        if value >= 0.20:
            return "orange"
        return "green"

    def _build_para_risk_push_card(self, now: Optional[float] = None) -> Optional[Dict[str, Any]]:
        stats = self._para_risk_stats
        if stats is None:
            return None

        authority = self._compute_para_authority_values()
        buffered_capacity: Decimal = authority.get("risk_capacity_buffered") or Decimal("0")
        worst_loss: Decimal = authority.get("worst_loss") or Decimal("0")
        ratio: Optional[float] = authority.get("ratio")
        capacity_note = authority.get("buffer_note")
        capacity_status = authority.get("buffer_status")

        ratio_text = _format_percent((ratio or 0.0) * 100, 2) if ratio is not None else "-"
        authority_inputs = authority.get("inputs") if isinstance(authority, dict) else None
        if not isinstance(authority_inputs, dict):
            authority_inputs = {}
        worst_label = authority_inputs.get("worst_account_label") or stats.worst_account_label or "-"
        worst_agent = authority_inputs.get("worst_agent_id") or stats.worst_agent_id or "-"
        color = self._feishu_risk_color(ratio)

        fields: List[Dict[str, Any]] = [
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**风险裕量**\n{_format_decimal(buffered_capacity or Decimal('0'), 2)}",
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**最坏亏损**\n{_format_decimal(worst_loss, 2)}",
                },
            },
            {
                "is_short": False,
                "text": {
                    "tag": "lark_md",
                    "content": f"**最坏账户**\n{worst_label} ({worst_agent})",
                },
            },
        ]

        if capacity_note:
            fields.append(
                {
                    "is_short": False,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**备注**\n{capacity_note} ({capacity_status})",
                    },
                }
            )

        # Feishu/Lark interactive card payload.
        header_title = f"[PARA] 风险播报 · RISK LEVEL: {ratio_text}"
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": color,
                "title": {"tag": "plain_text", "content": header_title},
            },
            "elements": [
                {"tag": "div", "fields": fields},
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": f"source: frontend_authority · ts: {int((now or time.time()))}",
                        }
                    ],
                },
            ],
        }
        return {"msg_type": "interactive", "card": card}

    def _compute_bp_authority_values(self) -> Optional[Dict[str, Any]]:
        """Best-effort Backpack snapshot computed from coordinator state.

        Data source: each agent's HedgeState.backpack_accounts (summary/accounts/positions).
        We keep it defensive since Backpack monitor payloads can vary.
        """

        equity_sum = Decimal("0")
        equity_available_sum = Decimal("0")
        total_pnl = Decimal("0")
        worst_pnl: Optional[Decimal] = None
        worst_agent_id: Optional[str] = None
        worst_account_label: Optional[str] = None
        max_initial_margin: Optional[Decimal] = None
        im_source: Optional[str] = None
        any_seen = False
        latest_update: Optional[float] = None

        def _coerce_ts(value: Any) -> Optional[float]:
            if value is None:
                return None
            try:
                ts = float(value)
            except Exception:
                return None
            if not math.isfinite(ts) or ts <= 0:
                return None
            # tolerate ms timestamps
            if ts > 1e12:
                ts = ts / 1000.0
            return ts

        for agent_id, state in self._states.items():
            bp = state.backpack_accounts
            if not isinstance(bp, dict):
                continue
            any_seen = True

            summary = bp.get("summary") if isinstance(bp.get("summary"), dict) else None

            # Track latest update timestamp (used for capacity buffer stability).
            ts_raw = None
            if isinstance(summary, dict):
                ts_raw = summary.get("updated_at")
            if ts_raw is None:
                ts_raw = bp.get("updated_at")
            safe_ts = _coerce_ts(ts_raw)
            if safe_ts is not None and (latest_update is None or safe_ts > latest_update):
                latest_update = safe_ts
            collateral = (
                summary.get("collateral")
                if isinstance(summary, dict) and isinstance(summary.get("collateral"), dict)
                else None
            )
            if isinstance(collateral, dict):
                eq = self._decimal_from(collateral.get("netEquity"))
                eq_avail = self._decimal_from(collateral.get("netEquityAvailable"))
                if eq is not None:
                    equity_sum += eq
                if eq_avail is not None:
                    equity_available_sum += eq_avail

            # Initial margin: best-effort for dashboard-aligned risk capacity.
            # Dashboard uses max(IM) across accounts/sources; we approximate by scanning
            # summary/accounts/positions-ish fields when available.
            candidate_im: Optional[Decimal] = None
            candidate_im_source: Optional[str] = None
            if isinstance(summary, dict):
                margin_block = summary.get("margin")
                if isinstance(margin_block, dict):
                    candidate_im = (
                        self._decimal_from(margin_block.get("initialMargin"))
                        or self._decimal_from(margin_block.get("initialMarginTotal"))
                        or self._decimal_from(margin_block.get("initial_margin"))
                    )
                    if candidate_im is not None:
                        candidate_im_source = "summary.margin"
                if candidate_im is None:
                    candidate_im = (
                        self._decimal_from(summary.get("initialMargin"))
                        or self._decimal_from(summary.get("initialMarginTotal"))
                        or self._decimal_from(summary.get("initial_margin"))
                    )
                    if candidate_im is not None:
                        candidate_im_source = "summary"
            if candidate_im is None:
                accounts = bp.get("accounts")
                if isinstance(accounts, list) and accounts:
                    for acc in accounts:
                        if not isinstance(acc, dict):
                            continue
                        im_val = (
                            # Align with dashboard: accountObj.initial_margin_requirement
                            self._decimal_from(acc.get("initial_margin_requirement"))
                            or self._decimal_from(acc.get("initialMarginRequirement"))
                            or self._decimal_from(acc.get("initialMarginReq"))
                            or self._decimal_from(acc.get("initialMargin"))
                            or self._decimal_from(acc.get("initialMarginTotal"))
                            or self._decimal_from(acc.get("initial_margin"))
                        )
                        if im_val is None:
                            continue
                        if candidate_im is None or im_val > candidate_im:
                            candidate_im = im_val
                            candidate_im_source = "accounts"

            # Dashboard also supports summary.initial_margin_requirement as a fallback.
            if candidate_im is None and isinstance(summary, dict):
                candidate_im = (
                    self._decimal_from(summary.get("initial_margin_requirement"))
                    or self._decimal_from(summary.get("initialMarginRequirement"))
                    or self._decimal_from(summary.get("initialMarginReq"))
                )
                if candidate_im is not None:
                    candidate_im_source = "summary.initial_margin_requirement"

            # Backpack monitor payload: user-confirmed IM stored as collateral.netEquityLocked.
            # When available, treat it as IM for Feishu risk push consumption.
            if candidate_im is None and isinstance(collateral, dict):
                candidate_im = self._decimal_from(collateral.get("netEquityLocked"))
                if candidate_im is not None:
                    candidate_im_source = "collateral.netEquityLocked"

            if candidate_im is not None:
                if max_initial_margin is None or candidate_im > max_initial_margin:
                    max_initial_margin = candidate_im
                    im_source = candidate_im_source

            # Account label (best-effort): first account name/id.
            account_label = "-"
            accounts = bp.get("accounts")
            if isinstance(accounts, list) and accounts:
                first = accounts[0]
                if isinstance(first, dict):
                    label_raw = first.get("account") or first.get("name") or first.get("id")
                    if label_raw is not None:
                        account_label = str(label_raw)

            agent_pnl = Decimal("0")
            positions = bp.get("positions")
            if isinstance(positions, list) and positions:
                for pos in positions:
                    if not isinstance(pos, dict):
                        continue
                    pnl = (
                        self._decimal_from(pos.get("pnlRealized"))
                        or self._decimal_from(pos.get("pnl"))
                        or self._decimal_from(pos.get("pnlUnrealized"))
                        or self._decimal_from(pos.get("unrealizedPnl"))
                        or self._decimal_from(pos.get("unrealized_pnl"))
                    )
                    if pnl is not None:
                        agent_pnl += pnl
            else:
                if isinstance(summary, dict):
                    pnl_fallback = (
                        self._decimal_from(summary.get("total_pnl"))
                        or self._decimal_from(summary.get("pnl"))
                        or self._decimal_from(summary.get("unrealized_pnl"))
                    )
                    if pnl_fallback is not None:
                        agent_pnl += pnl_fallback

            total_pnl += agent_pnl
            if worst_pnl is None or agent_pnl < worst_pnl:
                worst_pnl = agent_pnl
                worst_agent_id = agent_id
                worst_account_label = account_label

        if not any_seen:
            return None

        # Compute raw capacity using dashboard-aligned formula: Equity - 1.5×max(IM)
        max_im_value = max_initial_margin
        raw_capacity: Optional[Decimal] = None
        if max_im_value is not None and max_im_value > 0 and equity_sum > 0:
            raw_capacity = equity_sum - (Decimal("1.5") * max_im_value)

        # Apply PARA-like buffering to reduce flicker / temporary missing IM.
        buffered_capacity, buffer_note, buffer_status = _evaluate_risk_capacity_buffer(
            has_value=raw_capacity is not None and raw_capacity > 0,
            value=raw_capacity if raw_capacity is not None and raw_capacity > 0 else None,
            timestamp=latest_update,
            base_note="",
            state=self._bp_risk_capacity_buffer,
        )
        effective_capacity = buffered_capacity if buffered_capacity is not None else raw_capacity

        return {
            "equity_sum": equity_sum,
            "equity_available_sum": equity_available_sum,
            "total_pnl": total_pnl,
            "worst_pnl": worst_pnl if worst_pnl is not None else Decimal("0"),
            "worst_agent_id": worst_agent_id,
            "worst_account_label": worst_account_label,
            "max_initial_margin": max_initial_margin,
            "im_source": im_source,
            "risk_capacity_raw": raw_capacity if raw_capacity is not None and raw_capacity > 0 else None,
            "risk_capacity_buffered": effective_capacity if effective_capacity is not None and effective_capacity > 0 else None,
            "buffer_note": buffer_note,
            "buffer_status": buffer_status,
            "latest_update": latest_update,
        }

    def _build_bp_risk_push_card(self, now: Optional[float] = None) -> Optional[Dict[str, Any]]:
        values = self._compute_bp_authority_values()
        if not values:
            return None

        equity_sum: Decimal = values.get("equity_sum") or Decimal("0")
        equity_avail: Decimal = values.get("equity_available_sum") or Decimal("0")
        total_pnl: Decimal = values.get("total_pnl") or Decimal("0")
        worst_pnl: Decimal = values.get("worst_pnl") or Decimal("0")
        worst_agent = values.get("worst_agent_id") or "-"
        worst_label = values.get("worst_account_label") or "-"
        max_im: Optional[Decimal] = values.get("max_initial_margin")
        capacity_note = values.get("buffer_note")
        capacity_status = values.get("buffer_status")

        # Dashboard-aligned capacity and risk ratio:
        # capacity = equity_sum - 1.5 * max_initial_margin
        # ratio = worst_loss / capacity, worst_loss = max(0, -worst_pnl)
        # Prefer buffered capacity (PARA-like) to reduce flicker.
        capacity: Optional[Decimal] = values.get("risk_capacity_buffered") or None
        if capacity is None:
            capacity = values.get("risk_capacity_raw") or None
        if capacity is not None and capacity <= 0:
            capacity = None

        ratio: Optional[float] = None
        try:
            if capacity is not None and capacity > 0:
                loss = abs(worst_pnl) if worst_pnl < 0 else Decimal("0")
                ratio = float(loss / capacity)
        except Exception:
            ratio = None

        ratio_text = _format_percent((ratio or 0.0) * 100, 2) if ratio is not None else "-"
        color = self._feishu_risk_color(ratio)

        fields: List[Dict[str, Any]] = [
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**总权益(netEquity)**\n{_format_decimal(equity_sum, 2)}",
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**可用权益(netEquityAvailable)**\n{_format_decimal(equity_avail, 2)}",
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**持仓PnL合计**\n{_format_decimal(total_pnl, 2)}",
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**风险裕量(capacity)**\n{_format_decimal(capacity, 2)}"
                        if capacity is not None
                        else "**风险裕量(capacity)**\n-"
                    ),
                },
            },
            {
                "is_short": False,
                "text": {
                    "tag": "lark_md",
                    "content": f"**最坏账户(PnL)**\n{worst_label} ({worst_agent}) · {_format_decimal(worst_pnl, 2)}",
                },
            },
        ]

        if capacity_note:
            fields.append(
                {
                    "is_short": False,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**备注**\n{capacity_note} ({capacity_status})",
                    },
                }
            )

        header_title = f"[Backpack] 风险播报 · RISK LEVEL: {ratio_text}"
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": color,
                "title": {"tag": "plain_text", "content": header_title},
            },
            "elements": [
                {"tag": "div", "fields": fields},
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": f"source: coordinator_state · ts: {int((now or time.time()))}",
                        }
                    ],
                },
            ],
        }
        return {"msg_type": "interactive", "card": card}

    async def _try_send_feishu_bp_snapshot(self) -> None:
        if not self._feishu_webhook_url or not self._feishu_bp_push_enabled:
            return
        session = self._feishu_session
        if session is None or session.closed:
            self._feishu_session = ClientSession(timeout=ClientTimeout(total=10.0))
            session = self._feishu_session

        async with self._lock:
            payload = self._build_bp_risk_push_card()
        if not payload:
            return
        try:
            async with session.post(self._feishu_webhook_url, json=payload) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    LOGGER.warning(
                        "Feishu webhook push failed (Backpack, HTTP %s): %s",
                        resp.status,
                        body[:200],
                    )
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Feishu webhook push error (Backpack): %s", exc)

    async def _try_send_feishu_para_snapshot(self) -> None:
        if not self._feishu_webhook_url or not self._feishu_para_push_enabled:
            return
        session = self._feishu_session
        if session is None or session.closed:
            self._feishu_session = ClientSession(timeout=ClientTimeout(total=10.0))
            session = self._feishu_session

        async with self._lock:
            payload = self._build_para_risk_push_card()
        if not payload:
            return
        try:
            async with session.post(self._feishu_webhook_url, json=payload) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    LOGGER.warning("Feishu webhook push failed (HTTP %s): %s", resp.status, body[:200])
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Feishu webhook push error: %s", exc)

    def _apply_alert_settings(self, settings: Optional[RiskAlertSettings] = None) -> None:
        if settings is not None:
            self._alert_settings = settings.normalized()
            self._alert_settings_updated_at = time.time()
        config = self._alert_settings
        self._risk_alert_threshold = config.threshold
        self._risk_alert_reset = config.reset_ratio
        self._risk_alert_cooldown = config.cooldown
        if config.bark_url:
            # 强制 URL 直推模式（不携带标题/正文）
            self._bark_notifier = BarkNotifier(
                config.bark_url,
                append_payload=False,
                timeout=config.bark_timeout,
            )
        else:
            self._bark_notifier = None

    def _apply_para_alert_settings(self, settings: Optional[RiskAlertSettings] = None) -> None:
        if settings is not None:
            self._para_alert_settings = settings.normalized()
            self._para_alert_settings_updated_at = time.time()
        config = self._para_alert_settings
        self._para_risk_alert_threshold = config.threshold
        self._para_risk_alert_reset = config.reset_ratio
        self._para_risk_alert_cooldown = config.cooldown
        if config.bark_url:
            self._para_bark_notifier = BarkNotifier(
                config.bark_url,
                append_payload=False,
                timeout=config.bark_timeout,
            )
        else:
            self._para_bark_notifier = None

    def _apply_bp_alert_settings(self, settings: Optional[RiskAlertSettings] = None) -> None:
        if settings is not None:
            self._bp_alert_settings = settings.normalized()
            self._bp_alert_settings_updated_at = time.time()
        config = self._bp_alert_settings
        self._bp_risk_alert_threshold = config.threshold
        self._bp_risk_alert_reset = config.reset_ratio
        self._bp_risk_alert_cooldown = config.cooldown
        if config.bark_url:
            self._bp_bark_notifier = BarkNotifier(
                config.bark_url,
                append_payload=False,
                timeout=config.bark_timeout,
            )
        else:
            self._bp_bark_notifier = None

    def _alert_settings_payload(self, now: Optional[float] = None) -> Dict[str, Any]:
        payload = self._alert_settings.to_payload()
        payload["updated_at"] = self._alert_settings_updated_at
        payload["active"] = bool(self._risk_alert_active.get(GLOBAL_RISK_ALERT_KEY))
        last_alert = self._risk_alert_last_ts.get(GLOBAL_RISK_ALERT_KEY)
        payload["last_alert_at"] = last_alert
        cooldown = self._risk_alert_cooldown or 0.0
        if last_alert is not None and cooldown > 0:
            ready_at = last_alert + cooldown
            payload["cooldown_ready_at"] = ready_at
            current = now if now is not None else time.time()
            payload["cooldown_remaining"] = max(0.0, ready_at - current)
        else:
            payload["cooldown_ready_at"] = None
            payload["cooldown_remaining"] = 0.0
        return payload

    def _para_alert_settings_payload(self, now: Optional[float] = None) -> Dict[str, Any]:
        payload = self._para_alert_settings.to_payload()
        payload["updated_at"] = self._para_alert_settings_updated_at
        payload["active"] = bool(self._risk_alert_active.get(PARA_RISK_ALERT_KEY))
        last_alert = self._risk_alert_last_ts.get(PARA_RISK_ALERT_KEY)
        payload["last_alert_at"] = last_alert
        cooldown = self._para_risk_alert_cooldown or 0.0
        if last_alert is not None and cooldown > 0:
            ready_at = last_alert + cooldown
            payload["cooldown_ready_at"] = ready_at
            current = now if now is not None else time.time()
            payload["cooldown_remaining"] = max(0.0, ready_at - current)
        else:
            payload["cooldown_ready_at"] = None
            payload["cooldown_remaining"] = 0.0
        return payload

    def _bp_alert_settings_payload(self, now: Optional[float] = None) -> Dict[str, Any]:
        payload = self._bp_alert_settings.to_payload()
        payload["updated_at"] = self._bp_alert_settings_updated_at
        payload["active"] = bool(self._risk_alert_active.get(BP_RISK_ALERT_KEY))
        last_alert = self._risk_alert_last_ts.get(BP_RISK_ALERT_KEY)
        payload["last_alert_at"] = last_alert
        cooldown = self._bp_risk_alert_cooldown or 0.0
        if last_alert is not None and cooldown > 0:
            ready_at = last_alert + cooldown
            payload["cooldown_ready_at"] = ready_at
            current = now if now is not None else time.time()
            payload["cooldown_remaining"] = max(0.0, ready_at - current)
        else:
            payload["cooldown_ready_at"] = None
            payload["cooldown_remaining"] = 0.0
        return payload

    async def alert_settings_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return self._alert_settings_payload()

    async def para_alert_settings_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return self._para_alert_settings_payload()

    async def bp_alert_settings_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return self._bp_alert_settings_payload()

    async def apply_alert_settings(self, settings: RiskAlertSettings) -> Dict[str, Any]:
        async with self._lock:
            self._apply_alert_settings(settings)
            self._risk_alert_active.clear()
            self._risk_alert_last_ts.clear()
            return self._alert_settings_payload()

    async def apply_para_alert_settings(self, settings: RiskAlertSettings) -> Dict[str, Any]:
        async with self._lock:
            self._apply_para_alert_settings(settings)
            self._risk_alert_active.pop(PARA_RISK_ALERT_KEY, None)
            self._risk_alert_last_ts.pop(PARA_RISK_ALERT_KEY, None)
            return self._para_alert_settings_payload()

    async def apply_bp_alert_settings(self, settings: RiskAlertSettings) -> Dict[str, Any]:
        async with self._lock:
            self._apply_bp_alert_settings(settings)
            self._risk_alert_active.pop(BP_RISK_ALERT_KEY, None)
            self._risk_alert_last_ts.pop(BP_RISK_ALERT_KEY, None)
            return self._bp_alert_settings_payload()

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

    def _recalculate_global_risk(self) -> None:
        self._global_risk_stats = self._calculate_global_risk()
        self._para_risk_stats = self._calculate_para_risk()

    def _calculate_global_risk(self) -> Optional[GlobalRiskSnapshot]:
        total_equity_sum = Decimal("0")
        total_initial_margin_sum = Decimal("0")
        worst_loss_value = Decimal("0")
        worst_agent_id: Optional[str] = None
        worst_account_label: Optional[str] = None

        for agent_id, state in self._states.items():
            for payload in (state.grvt_accounts, state.paradex_accounts):
                if not isinstance(payload, dict):
                    continue
                summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else None
                for _, account_label, account_payload in self._flatten_grvt_accounts(agent_id, payload):
                    equity_value = self._select_equity_value(account_payload, summary)
                    if equity_value is None or equity_value <= 0:
                        continue
                    total_equity_sum += equity_value
                    total_pnl = self._decimal_from(account_payload.get("total_pnl"))
                    if total_pnl is None:
                        total_pnl = self._decimal_from(account_payload.get("total"))
                    initial_margin = self._compute_initial_margin_total(account_payload)
                    total_initial_margin_sum += initial_margin
                    transferable = self._compute_transferable_amount(equity_value, initial_margin, total_pnl)
                    if total_pnl is not None and total_pnl < 0:
                        abs_loss = abs(total_pnl)
                        if abs_loss > worst_loss_value:
                            worst_loss_value = abs_loss
                            worst_agent_id = agent_id
                            worst_account_label = account_label

        risk_capacity = total_equity_sum - total_initial_margin_sum

        if worst_loss_value <= 0 or risk_capacity <= 0:
            snapshot = GlobalRiskSnapshot(
                ratio=None,
                total_transferable=risk_capacity if risk_capacity > 0 else Decimal("0"),
                worst_loss_value=worst_loss_value,
                worst_agent_id=worst_agent_id,
                worst_account_label=worst_account_label,
            )
            self._record_transferable_history(snapshot.total_transferable)
            return snapshot

        ratio = float(worst_loss_value / risk_capacity)
        snapshot = GlobalRiskSnapshot(
            ratio=ratio,
            total_transferable=risk_capacity,
            worst_loss_value=worst_loss_value,
            worst_agent_id=worst_agent_id,
            worst_account_label=worst_account_label,
        )
        self._record_transferable_history(snapshot.total_transferable)
        return snapshot

    def _calculate_para_risk(self) -> Optional[ParaRiskSnapshot]:
        total_equity_sum = Decimal("0")
        max_initial_margin: Optional[Decimal] = None
        account_count = 0
        worst_loss_value = Decimal("0")
        worst_agent_id: Optional[str] = None
        worst_account_label: Optional[str] = None
        pnl_abs_records: List[Tuple[Decimal, str, str]] = []  # (abs_pnl, agent_id, account_label)

        for agent_id, state in self._states.items():
            payload = state.paradex_accounts
            if not isinstance(payload, dict):
                continue
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else None
            for _, account_label, account_payload in self._flatten_grvt_accounts(agent_id, payload):
                equity_value = self._select_equity_value(account_payload, summary)
                if equity_value is None or equity_value <= 0:
                    continue
                total_equity_sum += equity_value
                account_count += 1
                total_pnl = self._decimal_from(account_payload.get("total_pnl"))
                if total_pnl is None:
                    total_pnl = self._decimal_from(account_payload.get("total"))
                # IMPORTANT: Keep PARA risk capacity aligned with what the dashboard shows.
                # Dashboard uses per-account `initialMarginTotal` derived from snapshot fields
                # (not the margin schedule calculation). Prefer a direct `initial_margin` field
                # if provided; fall back to summing positions when missing.
                initial_margin = self._decimal_from(account_payload.get("initial_margin"))
                if initial_margin is None:
                    initial_margin = self._compute_initial_margin_total(account_payload)
                if max_initial_margin is None or initial_margin > max_initial_margin:
                    max_initial_margin = initial_margin
                if total_pnl is not None:
                    pnl_abs_records.append((abs(total_pnl), agent_id, account_label))

        # Loss definition for PARA: when there are exactly 2 accounts, use
        # max(|PnL_account_1|, |PnL_account_2|) regardless of sign.
        # Otherwise (unexpected account count), fall back to max abs(PnL) across accounts.
        if pnl_abs_records:
            if len(pnl_abs_records) == 2:
                abs_loss, agent_id, account_label = max(pnl_abs_records, key=lambda item: item[0])
                worst_loss_value = abs_loss
                worst_agent_id = agent_id
                worst_account_label = account_label
            else:
                abs_loss, agent_id, account_label = max(pnl_abs_records, key=lambda item: item[0])
                worst_loss_value = abs_loss
                worst_agent_id = agent_id
                worst_account_label = account_label

        max_im = max_initial_margin if max_initial_margin is not None else Decimal("0")
        risk_capacity = total_equity_sum - (Decimal("1.5") * max_im)

        if worst_loss_value <= 0 or risk_capacity <= 0:
            return ParaRiskSnapshot(
                ratio=None,
                risk_capacity=risk_capacity if risk_capacity > 0 else Decimal("0"),
                worst_loss_value=worst_loss_value,
                worst_agent_id=worst_agent_id,
                worst_account_label=worst_account_label,
                equity_sum=total_equity_sum,
                max_initial_margin=max_im,
                account_count=account_count,
                computed_at=time.time(),
            )

        ratio = float(worst_loss_value / risk_capacity)
        return ParaRiskSnapshot(
            ratio=ratio,
            risk_capacity=risk_capacity,
            worst_loss_value=worst_loss_value,
            worst_agent_id=worst_agent_id,
            worst_account_label=worst_account_label,
            equity_sum=total_equity_sum,
            max_initial_margin=max_im,
            account_count=account_count,
            computed_at=time.time(),
        )

    def _compute_para_authority_inputs(self) -> Dict[str, Any]:
        """Compute PARA risk components using the same field preference as the dashboard.

        Frontend logic (hedge_dashboard.html):
        - equity: account.equity ?? account.available_equity (or summary equivalents)
        - initial margin: account.initial_margin_requirement preferred; fallback to positions-based estimate
        - maxIM: max(per-account initial margin)
        - worst loss: max(abs(total_pnl)) for negative pnl
        """

        equity_sum = Decimal("0")
        max_im: Optional[Decimal] = None
        account_count = 0
        worst_loss = Decimal("0")
        worst_agent_id: Optional[str] = None
        worst_account_label: Optional[str] = None
        latest_update: Optional[float] = None
        im_source = "initial_margin_requirement"

        def _coerce_ts(value: Any) -> Optional[float]:
            if value is None:
                return None
            try:
                ts = float(value)
            except Exception:
                return None
            if not math.isfinite(ts) or ts <= 0:
                return None
            # tolerate ms timestamps
            if ts > 1e12:
                ts = ts / 1000.0
            return ts

        for agent_id, state in self._states.items():
            payload = state.paradex_accounts
            if not isinstance(payload, dict):
                continue
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else None
            ts_raw = None
            if summary is not None:
                ts_raw = summary.get("updated_at")
            if ts_raw is None:
                ts_raw = payload.get("updated_at")
            safe_ts = _coerce_ts(ts_raw)
            if safe_ts is not None and (latest_update is None or safe_ts > latest_update):
                latest_update = safe_ts

            # Account-path first (preferred in dashboard).
            for _, account_label, account_payload in self._flatten_grvt_accounts(agent_id, payload):
                equity_value = self._select_equity_value(account_payload, summary)
                if equity_value is None or equity_value <= 0:
                    continue
                equity_sum += equity_value
                account_count += 1

                total_pnl = self._decimal_from(account_payload.get("total_pnl"))
                if total_pnl is None:
                    total_pnl = self._decimal_from(account_payload.get("total"))
                if total_pnl is not None and total_pnl < 0:
                    abs_loss = abs(total_pnl)
                    if abs_loss > worst_loss:
                        worst_loss = abs_loss
                        worst_agent_id = agent_id
                        worst_account_label = account_label

                # Dashboard uses `initial_margin_requirement` when available.
                # Monitor may also populate `initial_margin` / `initialMarginTotal`; prefer the
                # authority field, then fall back.
                initial_margin = self._decimal_from(account_payload.get("initial_margin_requirement"))
                if initial_margin is None or initial_margin <= 0:
                    fallback_margin = self._decimal_from(account_payload.get("initial_margin"))
                    if fallback_margin is None:
                        fallback_margin = self._decimal_from(account_payload.get("initialMarginTotal"))
                    if fallback_margin is not None and fallback_margin > 0:
                        im_source = "initial_margin(fallback)"
                        initial_margin = fallback_margin

                if initial_margin is None or initial_margin <= 0:
                    # fallback: per-position IM sum (same spirit as dashboard)
                    positions = account_payload.get("positions")
                    if isinstance(positions, list) and positions:
                        im_source = "positions"
                        initial_margin = Decimal("0")
                        for pos in positions:
                            if not isinstance(pos, dict):
                                continue
                            im = self._compute_position_initial_margin(pos)
                            if im is not None and im > 0:
                                initial_margin += im
                if initial_margin is None:
                    initial_margin = Decimal("0")

                if max_im is None or initial_margin > max_im:
                    max_im = initial_margin

        max_im_value = max_im if max_im is not None else Decimal("0")
        raw_risk_capacity = equity_sum - (Decimal("1.5") * max_im_value)
        return {
            "computed_at": time.time(),
            "latest_update": latest_update,
            "equity_sum": equity_sum,
            "max_initial_margin": max_im_value,
            "account_count": account_count,
            "worst_loss": worst_loss,
            "worst_agent_id": worst_agent_id,
            "worst_account_label": worst_account_label,
            "risk_capacity_raw": raw_risk_capacity if raw_risk_capacity > 0 else Decimal("0"),
            "im_source": im_source,
        }

    def _compute_para_authority_values(self) -> Dict[str, Any]:
        """Compute the dashboard-authoritative PARA loss/base values.

        Returns normalized numbers + formatted strings ready for history.
        """
        inputs = self._compute_para_authority_inputs()
        latest_update = inputs.get("latest_update")
        ts_for_buffer = None
        if isinstance(latest_update, (int, float)) and math.isfinite(float(latest_update)) and float(latest_update) > 0:
            ts_for_buffer = float(latest_update)

        raw_capacity: Decimal = inputs.get("risk_capacity_raw") or Decimal("0")
        buffered_capacity, buffer_note, buffer_status = _evaluate_risk_capacity_buffer(
            has_value=raw_capacity is not None and raw_capacity > 0,
            value=raw_capacity if raw_capacity > 0 else None,
            timestamp=ts_for_buffer,
            base_note="",
            state=self._para_risk_capacity_buffer,
        )
        effective_capacity = buffered_capacity if buffered_capacity is not None else raw_capacity

        worst_loss: Decimal = inputs.get("worst_loss") or Decimal("0")
        ratio: Optional[float]
        if effective_capacity is None or effective_capacity <= 0 or worst_loss <= 0:
            ratio = None
        else:
            ratio = float(worst_loss / effective_capacity)

        return {
            "inputs": inputs,
            "risk_capacity_raw": raw_capacity,
            "risk_capacity_buffered": effective_capacity,
            "buffer_note": buffer_note,
            "buffer_status": buffer_status,
            "worst_loss": worst_loss,
            "ratio": ratio,
        }

    def _record_transferable_history(self, total_value: Decimal) -> None:
        if total_value is None or total_value <= 0:
            return
        now = time.time()
        if self._transferable_history:
            last_ts, _ = self._transferable_history[-1]
            if now - last_ts <= TRANSFERABLE_HISTORY_MERGE_SECONDS:
                self._transferable_history[-1] = (last_ts, total_value)
                return
        self._transferable_history.append((now, total_value))

    def _serialize_transferable_history(self) -> List[Dict[str, Any]]:
        if not self._transferable_history:
            return []
        payload: List[Dict[str, Any]] = []
        for ts, value in self._transferable_history:
            try:
                total_text = format(value, "f")
            except Exception:
                total_text = str(value)
            payload.append({
                "ts": ts,
                "total": total_text,
            })
        return payload

    def _compute_transferable_for_agent(
        self,
        agent_id: str,
        grvt_payload: Optional[Dict[str, Any]],
    ) -> Optional[Decimal]:
        if not isinstance(grvt_payload, dict):
            return None
        summary = grvt_payload.get("summary")
        summary_block = summary if isinstance(summary, dict) else None
        total = Decimal("0")
        has_value = False
        for _, _, account_payload in self._flatten_grvt_accounts(agent_id, grvt_payload):
            equity_value = self._select_equity_value(account_payload, summary_block)
            if equity_value is None or equity_value <= 0:
                continue
            total_pnl = self._decimal_from(account_payload.get("total_pnl"))
            if total_pnl is None:
                total_pnl = self._decimal_from(account_payload.get("total"))
            initial_margin = self._compute_initial_margin_total(account_payload)
            transferable = self._compute_transferable_amount(equity_value, initial_margin, total_pnl)
            if transferable is None:
                continue
            total += transferable
            has_value = True
        if not has_value:
            return None
        return total

    @staticmethod
    def _select_equity_value(primary: Dict[str, Any], summary: Optional[Dict[str, Any]]) -> Optional[Decimal]:
        for source in (primary, summary or {}):
            if not isinstance(source, dict):
                continue
            for field in ("equity", "available_equity"):
                value = HedgeCoordinator._decimal_from(source.get(field))
                if value is not None and value > 0:
                    return value
        return None

    @staticmethod
    def _compute_initial_margin_total(account_payload: Dict[str, Any]) -> Decimal:
        positions = account_payload.get("positions")
        if not isinstance(positions, list):
            return Decimal("0")
        total = Decimal("0")
        for position in positions:
            if not isinstance(position, dict):
                continue
            margin = HedgeCoordinator._compute_position_initial_margin(position)
            if margin is not None:
                total += margin
        return total

    @staticmethod
    def _compute_transferable_amount(
        equity_value: Optional[Decimal],
        initial_margin: Decimal,
        total_pnl: Optional[Decimal],
    ) -> Optional[Decimal]:
        if equity_value is None:
            return None
        unrealized = Decimal("0")
        if total_pnl is not None and total_pnl > 0:
            unrealized = total_pnl
        available = equity_value - initial_margin - unrealized
        if available <= 0:
            return Decimal("0")
        return available

    @staticmethod
    def _compute_position_initial_margin(position: Dict[str, Any]) -> Optional[Decimal]:
        size = None
        for field in ("net_size", "size", "contracts", "amount"):
            size = HedgeCoordinator._decimal_from(position.get(field))
            if size is not None:
                break
        price = None
        for field in ("mark_price", "markPrice", "last_price", "entry_price", "entryPrice"):
            price = HedgeCoordinator._decimal_from(position.get(field))
            if price is not None:
                break
        if size is None or price is None:
            return None
        notional = abs(size * price)
        if notional <= 0:
            return None
        base_asset = HedgeCoordinator._extract_base_asset(position.get("symbol"))
        tier = HedgeCoordinator._resolve_margin_tier(base_asset, notional)
        if tier is None:
            return None
        initial_rate, _ = tier
        return notional * initial_rate

    @staticmethod
    def _normalize_symbol_label(value: Any) -> str:
        if value is None:
            return ""
        try:
            text = str(value).strip().upper()
        except Exception:
            return ""
        for token in ("/", "-", ":", "_", " "):
            text = text.replace(token, "")
        return text

    @staticmethod
    def _extract_base_asset(symbol: Any) -> Optional[str]:
        if symbol is None:
            return None
        try:
            text = str(symbol).strip().upper()
        except Exception:
            return None
        for token in (":", "-", "_", " "):
            text = text.replace(token, "/")
        parts = [part for part in text.split("/") if part]
        candidate = parts[0] if parts else text
        suffixes = ("PERP", "FUT", "FUTURES", "USD", "USDT", "USDC")
        stripped = True
        while stripped and candidate:
            stripped = False
            for suffix in suffixes:
                if candidate.endswith(suffix) and len(candidate) > len(suffix):
                    candidate = candidate[: -len(suffix)]
                    stripped = True
                    break
        candidate = "".join(ch for ch in candidate if ch.isalpha())
        return candidate or None

    @staticmethod
    def _resolve_margin_schedule(base_asset: Optional[str]) -> Tuple[Tuple[str, str, str], ...]:
        key = (base_asset or "").upper()
        return MARGIN_SCHEDULES.get(key, DEFAULT_MARGIN_SCHEDULE)

    @staticmethod
    def _resolve_margin_tier(
        base_asset: Optional[str],
        notional: Decimal,
    ) -> Optional[Tuple[Decimal, Decimal]]:
        schedule = HedgeCoordinator._resolve_margin_schedule(base_asset)
        for max_str, initial_str, maintenance_str in schedule:
            try:
                initial_rate = Decimal(initial_str)
                maintenance_rate = Decimal(maintenance_str)
            except Exception:
                continue
            if max_str == "Infinity":
                max_value = None
            else:
                try:
                    max_value = Decimal(max_str)
                except Exception:
                    continue
            if max_value is None or notional <= max_value:
                return initial_rate, maintenance_rate
        return None

    def _serialize_global_risk(self, stats: GlobalRiskSnapshot) -> Dict[str, Any]:
        return {
            "ratio": stats.ratio,
            "total_transferable": self._format_decimal(stats.total_transferable),
            "worst_loss": self._format_decimal(stats.worst_loss_value),
            "worst_agent_id": stats.worst_agent_id,
            "worst_account_label": stats.worst_account_label,
        }

    def _serialize_para_risk(self, stats: ParaRiskSnapshot) -> Dict[str, Any]:
        return {
            "ratio": stats.ratio,
            "risk_capacity": self._format_decimal(stats.risk_capacity),
            "worst_loss": self._format_decimal(stats.worst_loss_value),
            "worst_agent_id": stats.worst_agent_id,
            "worst_account_label": stats.worst_account_label,
        }

    async def alert_history_snapshot(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._alert_history_lock:
            entries = list(self._alert_history)
        if limit is not None and limit > 0:
            entries = entries[-limit:]
        # newest first for UI
        return [copy.deepcopy(entry) for entry in reversed(entries)]

    async def _record_alert_history(
        self,
        alert: RiskAlertInfo,
        *,
        source: str,
        status: str,
        title: str,
        body: str,
        error: Optional[str] = None,
    ) -> None:
        def _to_float(value: Decimal) -> Optional[float]:
            if value is None:
                return None
            try:
                return float(value)
            except Exception:
                return None

        if isinstance(alert.key, str) and alert.key.startswith("para_stale::"):
            kind = "para_stale"
        elif alert.key == PARA_RISK_ALERT_KEY:
            kind = "para_risk"
        elif alert.key == BP_RISK_ALERT_KEY:
            kind = "bp_risk"
        else:
            kind = "risk"

        entry = {
            "timestamp": time.time(),
            "kind": kind,
            "source": source,
            "status": status,
            "agent_id": alert.agent_id,
            "account_label": alert.account_label,
            "ratio": alert.ratio,
            "ratio_percent": alert.ratio * 100 if alert.ratio is not None else None,
            "loss_value": self._format_decimal(alert.loss_value),
            "loss_value_raw": _to_float(alert.loss_value),
            "base_value": self._format_decimal(alert.base_value),
            "base_value_raw": _to_float(alert.base_value),
            "base_label": alert.base_label or "wallet",
            "title": title,
            "body": body,
            "error": error,
        }

        if kind in {"para_risk", "bp_risk"}:
            # PARA history only needs the essential numbers for display: loss / capacity.
            # We intentionally avoid attaching extra forensics fields to keep the history light.
            pass
        async with self._alert_history_lock:
            self._alert_history.append(entry)

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
        agents_payload: Dict[str, Dict[str, Any]] = {}
        for agent_id, state in self._states.items():
            payload = state.serialize()
            transferable = self._compute_transferable_for_agent(agent_id, state.grvt_accounts)
            if transferable is not None:
                try:
                    payload["grvt_transferable_balance"] = format(transferable, "f")
                except Exception:
                    payload["grvt_transferable_balance"] = str(transferable)
            agents_payload[agent_id] = payload

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
        grvt_map = {
            agent_id: copy.deepcopy(state.grvt_accounts)
            for agent_id, state in self._states.items()
            if state.grvt_accounts
        }
        if grvt_map:
            snapshot["grvt_accounts"] = grvt_map
        paradex_map = {
            agent_id: copy.deepcopy(state.paradex_accounts)
            for agent_id, state in self._states.items()
            if state.paradex_accounts
        }
        if paradex_map:
            snapshot["paradex_accounts"] = paradex_map
        if self._global_risk_stats is None or self._para_risk_stats is None:
            self._recalculate_global_risk()
        if self._global_risk_stats is not None:
            snapshot["global_risk"] = self._serialize_global_risk(self._global_risk_stats)
        if self._para_risk_stats is not None:
            snapshot["para_risk"] = self._serialize_para_risk(self._para_risk_stats)
            transferable_history = self._serialize_transferable_history()
            if transferable_history:
                snapshot["transferable_history"] = transferable_history
        return snapshot

    async def update(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        alerts: Sequence[RiskAlertInfo] = []
        async with self._lock:
            agent_id = self._normalize_agent_id(payload.get("agent_id"))
            state = self._states.get(agent_id)
            if state is None:
                state = HedgeState(agent_id=agent_id)
                self._states[agent_id] = state

            now = time.time()
            state.update_from_payload(payload)
            self._recalculate_global_risk()
            alerts = self._prepare_risk_alerts(agent_id, state.grvt_accounts)
            alerts = list(alerts) + list(self._prepare_para_risk_alerts(agent_id))
            alerts = list(alerts) + list(self._prepare_bp_risk_alerts(agent_id))
            alerts = list(alerts) + list(self._prepare_para_stale_alerts(now))
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

    async def get_para_transfer_defaults(self, agent_id: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_agent_id(agent_id)
        async with self._lock:
            state = self._states.get(normalized)
            if not state:
                return None
            para_block = state.paradex_accounts
            if not isinstance(para_block, dict):
                return None
            defaults = para_block.get("transfer_defaults")
            if isinstance(defaults, dict):
                return copy.deepcopy(defaults)
            return None

    def _prepare_risk_alerts(self, agent_id: str, grvt_payload: Optional[Dict[str, Any]]) -> Sequence[RiskAlertInfo]:
        del grvt_payload  # unused in global risk evaluation
        if self._bark_notifier is None or self._risk_alert_threshold is None:
            return []

        stats = self._global_risk_stats
        if stats is None or stats.ratio is None:
            self._risk_alert_active.pop(GLOBAL_RISK_ALERT_KEY, None)
            return []

        threshold = self._risk_alert_threshold
        reset_ratio = self._risk_alert_reset if self._risk_alert_reset is not None else threshold * 0.7
        now = time.time()
        alerts: List[RiskAlertInfo] = []

        if (
            stats.ratio >= threshold
            and stats.total_transferable > 0
            and stats.worst_loss_value > 0
        ):
            already_active = self._risk_alert_active.get(GLOBAL_RISK_ALERT_KEY, False)
            last_ts = self._risk_alert_last_ts.get(GLOBAL_RISK_ALERT_KEY, 0.0)
            if not already_active and (now - last_ts) >= self._risk_alert_cooldown:
                alerts.append(
                    RiskAlertInfo(
                        key=GLOBAL_RISK_ALERT_KEY,
                        agent_id=stats.worst_agent_id or agent_id,
                        account_label=stats.worst_account_label or "Global",
                        ratio=stats.ratio,
                        loss_value=stats.worst_loss_value,
                        base_value=stats.total_transferable,
                        base_label="Equity-IM",
                    )
                )
                self._risk_alert_active[GLOBAL_RISK_ALERT_KEY] = True
                self._risk_alert_last_ts[GLOBAL_RISK_ALERT_KEY] = now
        else:
            if stats.ratio < reset_ratio:
                self._risk_alert_active.pop(GLOBAL_RISK_ALERT_KEY, None)

        return alerts

    def _prepare_para_risk_alerts(self, agent_id: str) -> Sequence[RiskAlertInfo]:
        # PARA risk alerts must be gated by PARA notifier/settings only.
        # Otherwise a missing *global* Bark config would incorrectly disable PARA alerts.
        if self._para_bark_notifier is None or self._para_risk_alert_threshold is None:
            if _env_debug(PARA_RISK_DEBUG_ENV, False):
                LOGGER.debug(
                    "[para_risk] skipped: notifier/threshold missing (notifier=%s threshold=%r)",
                    "set" if self._para_bark_notifier is not None else "missing",
                    self._para_risk_alert_threshold,
                )
            return []

        stats = self._para_risk_stats
        if stats is None or stats.ratio is None:
            if _env_debug(PARA_RISK_DEBUG_ENV, False):
                LOGGER.debug("[para_risk] skipped: stats missing (stats=%r)", stats)
            self._risk_alert_active.pop(PARA_RISK_ALERT_KEY, None)
            return []

        threshold = self._para_risk_alert_threshold
        reset_ratio = (
            self._para_risk_alert_reset
            if self._para_risk_alert_reset is not None
            else threshold * 0.7
        )
        now = time.time()
        alerts: List[RiskAlertInfo] = []

        # Authoritative values come from the dashboard algorithm (field preference + buffering).
        authority = self._compute_para_authority_values()
        authority_ratio = authority.get("ratio")
        authority_loss: Decimal = authority.get("worst_loss") or Decimal("0")
        authority_base: Decimal = authority.get("risk_capacity_buffered") or Decimal("0")

        if _env_debug(PARA_RISK_DEBUG_ENV, False):
            LOGGER.debug(
                "[para_risk] eval: threshold=%.4f ratio=%r base=%s loss=%s active=%s",
                float(threshold),
                authority_ratio,
                str(authority_base),
                str(authority_loss),
                bool(self._risk_alert_active.get(PARA_RISK_ALERT_KEY, False)),
            )

        if authority_ratio is not None and authority_ratio >= threshold and authority_base > 0 and authority_loss > 0:
            already_active = self._risk_alert_active.get(PARA_RISK_ALERT_KEY, False)
            last_ts = self._risk_alert_last_ts.get(PARA_RISK_ALERT_KEY, 0.0)
            cooldown = self._para_risk_alert_cooldown if self._para_risk_alert_cooldown else self._risk_alert_cooldown
            if not already_active and (now - last_ts) >= cooldown:
                alerts.append(
                    RiskAlertInfo(
                        key=PARA_RISK_ALERT_KEY,
                        agent_id=stats.worst_agent_id or agent_id,
                        account_label=stats.worst_account_label or "PARA",
                        ratio=float(authority_ratio),
                        loss_value=authority_loss,
                        base_value=authority_base,
                        base_label="risk_capacity(frontend_authority)",
                    )
                )
                self._risk_alert_active[PARA_RISK_ALERT_KEY] = True
                self._risk_alert_last_ts[PARA_RISK_ALERT_KEY] = now
                if _env_debug(PARA_RISK_DEBUG_ENV, False):
                    LOGGER.debug(
                        "[para_risk] ALERT queued: ratio=%.4f threshold=%.4f cooldown=%.2fs since_last=%.2fs",
                        float(authority_ratio),
                        float(threshold),
                        float(cooldown or 0.0),
                        float(now - last_ts),
                    )
            else:
                if _env_debug(PARA_RISK_DEBUG_ENV, False):
                    LOGGER.debug(
                        "[para_risk] blocked: active=%s since_last=%.2fs cooldown=%.2fs",
                        bool(already_active),
                        float(now - last_ts),
                        float(cooldown or 0.0),
                    )
        else:
            if stats.ratio < reset_ratio:
                self._risk_alert_active.pop(PARA_RISK_ALERT_KEY, None)

        return alerts

    def _prepare_bp_risk_alerts(self, agent_id: str) -> Sequence[RiskAlertInfo]:
        # Backpack risk alerts are scoped to BP notifier/settings only.
        if self._bp_bark_notifier is None or self._bp_risk_alert_threshold is None:
            return []

        threshold = self._bp_risk_alert_threshold
        reset_ratio = self._bp_risk_alert_reset if self._bp_risk_alert_reset is not None else threshold * 0.7
        now = time.time()
        alerts: List[RiskAlertInfo] = []

        authority = self._compute_bp_authority_values() or {}
        authority_ratio = authority.get("ratio")
        authority_loss: Decimal = authority.get("worst_loss") or Decimal("0")
        authority_base: Decimal = authority.get("risk_capacity_buffered") or Decimal("0")

        if authority_ratio is not None and authority_ratio >= threshold and authority_base > 0 and authority_loss > 0:
            already_active = self._risk_alert_active.get(BP_RISK_ALERT_KEY, False)
            last_ts = self._risk_alert_last_ts.get(BP_RISK_ALERT_KEY, 0.0)
            cooldown = self._bp_risk_alert_cooldown if self._bp_risk_alert_cooldown else self._risk_alert_cooldown
            if not already_active and (now - last_ts) >= cooldown:
                alerts.append(
                    RiskAlertInfo(
                        key=BP_RISK_ALERT_KEY,
                        agent_id=agent_id,
                        account_label="Backpack",
                        ratio=float(authority_ratio),
                        loss_value=authority_loss,
                        base_value=authority_base,
                        base_label="risk_capacity(frontend_authority)",
                    )
                )
                self._risk_alert_active[BP_RISK_ALERT_KEY] = True
                self._risk_alert_last_ts[BP_RISK_ALERT_KEY] = now
        else:
            if authority_ratio is None or authority_ratio < reset_ratio:
                self._risk_alert_active.pop(BP_RISK_ALERT_KEY, None)

        return alerts

    def _prepare_para_stale_alerts(self, now: float) -> Sequence[RiskAlertInfo]:
        # PARA stale alerts are also scoped to PARA notifier.
        if self._para_bark_notifier is None:
            if _env_debug(PARA_STALE_DEBUG_ENV, False):
                LOGGER.debug("[para_stale] skipped: para notifier missing")
            return []
        critical = max(float(self._para_stale_critical_seconds or 0), 0.0)
        if critical <= 0:
            if _env_debug(PARA_STALE_DEBUG_ENV, False):
                LOGGER.debug("[para_stale] skipped: critical<=0 (critical=%.2f)", critical)
            return []
        alerts: List[RiskAlertInfo] = []
        for agent_id, state in self._states.items():
            para_block = state.paradex_accounts
            if not isinstance(para_block, dict):
                if _env_debug(PARA_STALE_DEBUG_ENV, False):
                    LOGGER.debug(
                        "[para_stale] agent=%s skipped: paradex_accounts missing/non-dict (type=%s)",
                        agent_id,
                        type(para_block).__name__,
                    )
                continue
            summary = para_block.get("summary") if isinstance(para_block.get("summary"), dict) else None
            ts_raw = None
            if summary:
                ts_raw = summary.get("updated_at")
            if ts_raw is None:
                ts_raw = para_block.get("updated_at")
            if ts_raw is None:
                ts_raw = state.last_update_ts
            ts = None
            try:
                ts = float(ts_raw)
                if ts > 1e12:
                    ts = ts / 1000.0
            except Exception:
                ts = None
            if ts is None:
                if _env_debug(PARA_STALE_DEBUG_ENV, False):
                    LOGGER.debug(
                        "[para_stale] agent=%s skipped: ts parse failed (ts_raw=%r)",
                        agent_id,
                        ts_raw,
                    )
                continue
            age = now - ts
            if age < critical:
                if _env_debug(PARA_STALE_DEBUG_ENV, False):
                    LOGGER.debug(
                        "[para_stale] agent=%s ok: age<critical (age=%.2fs critical=%.2fs ts=%.3f now=%.3f)",
                        agent_id,
                        age,
                        critical,
                        ts,
                        now,
                    )
                continue
            key = f"para_stale::{agent_id}"
            last_ts = self._risk_alert_last_ts.get(key, 0.0)
            if (now - last_ts) < self._risk_alert_cooldown:
                if _env_debug(PARA_STALE_DEBUG_ENV, False):
                    LOGGER.debug(
                        "[para_stale] agent=%s skipped: cooldown (since_last=%.2fs cooldown=%.2fs last_ts=%.3f)",
                        agent_id,
                        now - last_ts,
                        self._risk_alert_cooldown,
                        last_ts,
                    )
                continue
            account_label = None
            if summary:
                account_label = summary.get("label") or summary.get("account_label") or summary.get("name")
            ratio = age / critical if critical > 0 else 1.0
            try:
                loss_value = Decimal(str(round(age, 2)))
                base_value = Decimal(str(round(critical, 2)))
            except Exception:
                loss_value = Decimal("0")
                base_value = Decimal(str(critical)) if critical else Decimal("0")
            alerts.append(
                RiskAlertInfo(
                    key=key,
                    agent_id=agent_id,
                    account_label=account_label or "PARA",
                    ratio=ratio,
                    loss_value=loss_value,
                    base_value=base_value,
                    base_label="stale_seconds",
                )
            )
            if _env_debug(PARA_STALE_DEBUG_ENV, False):
                LOGGER.debug(
                    "[para_stale] agent=%s ALERT queued: age=%.2fs critical=%.2fs ratio=%.4f label=%s",
                    agent_id,
                    age,
                    critical,
                    ratio,
                    (account_label or "PARA"),
                )
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
        if not alerts:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            LOGGER.warning("Unable to schedule Bark alert; event loop not running")
            return
        for alert in alerts:
            loop.create_task(self._send_risk_alert(alert, source="auto"))

    async def _send_risk_alert(self, alert: RiskAlertInfo, *, source: str = "auto") -> None:
        use_para = False
        use_bp = False
        if alert.key == PARA_RISK_ALERT_KEY:
            use_para = True
        elif isinstance(alert.key, str) and alert.key.startswith("para_stale::"):
            use_para = True
        elif alert.key == BP_RISK_ALERT_KEY:
            use_bp = True

        notifier: Optional[BarkNotifier]
        if use_para:
            notifier = self._para_bark_notifier
        elif use_bp:
            notifier = self._bp_bark_notifier
        else:
            notifier = self._bark_notifier
        if notifier is None:
            # Avoid permanently suppressing alerts when the notifier is misconfigured.
            if isinstance(alert.key, str) and alert.key.startswith("para_stale::"):
                # keep stale evaluation active so it can recover once notifier is configured
                self._risk_alert_last_ts.pop(alert.key, None)
            if _env_debug(PARA_RISK_DEBUG_ENV, False) and alert.key == PARA_RISK_ALERT_KEY:
                LOGGER.debug("[para_risk] delivery skipped: para notifier missing")
            return

        # Stale alerts use per-agent keys; record cooldown timestamp only when we will actually attempt delivery.
        if isinstance(alert.key, str) and alert.key.startswith("para_stale::"):
            self._risk_alert_last_ts[alert.key] = time.time()
        # Bark 只需 URL，不传标题/正文
        title = ""
        body = ""
        status = "sent"
        error_message = None
        try:
            if _env_debug(PARA_RISK_DEBUG_ENV, False) and use_para:
                LOGGER.debug("[para_risk] delivering alert key=%s source=%s", alert.key, source)
            await notifier.send(title=title, body=body)
        except Exception as exc:  # pragma: no cover - network path
            status = "error"
            error_message = str(exc)
            LOGGER.warning("Bark alert delivery failed: %s", exc)
        else:
            if _env_debug(PARA_RISK_DEBUG_ENV, False) and use_para:
                LOGGER.debug("[para_risk] delivery ok: key=%s source=%s", alert.key, source)
        finally:
            await self._record_alert_history(
                alert,
                source=source,
                status=status,
                title=title,
                body=body,
                error=error_message,
            )

    async def trigger_test_alert(self, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        overrides = overrides or {}
        async with self._lock:
            kind = None
            if isinstance(overrides.get("kind"), str):
                kind = str(overrides.get("kind") or "").strip().lower() or None
            use_para = kind in {"para", "para_risk", "para-risk"}

            notifier = self._para_bark_notifier if use_para else self._bark_notifier
            threshold = self._para_risk_alert_threshold if use_para else self._risk_alert_threshold
            if notifier is None or threshold is None:
                raise RuntimeError("Bark risk alerts are not enabled; configure Bark URL and threshold first.")
            stats = self._para_risk_stats if use_para else self._global_risk_stats
            ratio = overrides.get("ratio")
            if ratio is None and stats and stats.ratio:
                ratio = stats.ratio
            if ratio is None:
                ratio = threshold
            try:
                ratio = float(ratio)
            except (TypeError, ValueError):
                ratio = threshold
            if ratio <= 0:
                ratio = threshold
            base_value_raw = overrides.get("base_value")
            base_value = self._decimal_from(base_value_raw)
            if base_value is None and stats:
                candidate = None
                if use_para:
                    # For PARA test alerts, enforce dashboard-authoritative base value.
                    authority = self._compute_para_authority_values()
                    candidate = authority.get("risk_capacity_buffered")
                    if not isinstance(candidate, Decimal) or candidate <= 0:
                        # Unit tests may not populate paradex account payloads; fall back to stats.
                        candidate = getattr(stats, "risk_capacity", None)
                elif getattr(stats, "total_transferable", None):
                    candidate = getattr(stats, "total_transferable")
                if candidate:
                    base_value = cast(Decimal, candidate)
            if base_value is None:
                base_value = Decimal("100000")
            loss_value_raw = overrides.get("loss_value")
            loss_value = self._decimal_from(loss_value_raw)
            if loss_value is None:
                if use_para:
                    # For PARA test alerts, enforce dashboard-authoritative loss value.
                    authority = self._compute_para_authority_values()
                    candidate_loss = authority.get("worst_loss")
                    if isinstance(candidate_loss, Decimal) and candidate_loss > 0:
                        loss_value = candidate_loss
                    else:
                        try:
                            loss_value = Decimal(str(ratio)) * base_value
                        except Exception:
                            loss_value = Decimal("50000")
                else:
                    try:
                        loss_value = Decimal(str(ratio)) * base_value
                    except Exception:
                        loss_value = Decimal("50000")
            agent_id = (overrides.get("agent_id") or (stats.worst_agent_id if stats else None) or "test-agent")
            account_label = overrides.get("account_label") or (stats.worst_account_label if stats else None) or "Test Account"
            alert = RiskAlertInfo(
                # When testing PARA alerts, reuse the PARA key so history.kind becomes 'para_risk'
                # and the entry shows up in the PARA history table.
                key=PARA_RISK_ALERT_KEY if use_para else f"test::{agent_id}",
                agent_id=agent_id,
                account_label=account_label,
                ratio=ratio,
                loss_value=loss_value,
                base_value=base_value,
                base_label="risk_capacity(frontend_authority)" if use_para else "Equity-IM",
            )

        await self._send_risk_alert(alert, source="test")
        return {
            "agent_id": alert.agent_id,
            "account_label": alert.account_label,
            "ratio": alert.ratio,
            "ratio_percent": alert.ratio * 100.0,
            "loss_value": self._format_decimal(alert.loss_value),
            "base_value": self._format_decimal(alert.base_value),
            "base_label": alert.base_label,
        }


class CoordinatorApp:
    def __init__(
        self,
        *,
        dashboard_username: Optional[str] = None,
        dashboard_password: Optional[str] = None,
        dashboard_session_ttl: float = 7 * 24 * 3600,
        alert_settings: Optional[RiskAlertSettings] = None,
    ) -> None:
        self._coordinator = HedgeCoordinator(alert_settings=alert_settings)
        # Para/global alert scopes are controlled by CLI; do not force inheritance here.
        self._adjustments = GrvtAdjustmentManager()
        self._backpack_adjustments = BackpackAdjustmentManager()

        # Persisted Backpack history (adjust + internal transfer). We keep a small
        # server-side list so the dashboard can show history even after restart.
        self._bp_adjustments_history_limit: int = 500
        loaded_history = _load_json_file(BP_ADJUSTMENTS_HISTORY_FILE, default=[])
        self._bp_adjustments_history: List[Dict[str, Any]] = (
            loaded_history if isinstance(loaded_history, list) else []
        )
        if len(self._bp_adjustments_history) > self._bp_adjustments_history_limit:
            self._bp_adjustments_history = self._bp_adjustments_history[: self._bp_adjustments_history_limit]
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
        self._para_auto_balance_cfg: Optional[AutoBalanceConfig] = None
        self._para_auto_balance_lock = asyncio.Lock()
        self._para_auto_balance_cooldown_until: Optional[float] = None
        self._para_auto_balance_status: Dict[str, Any] = {
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
        self._para_auto_balance_task: Optional[asyncio.Task] = None

        # Backpack auto balance (equity equalization via internal transfer)
        self._bp_auto_balance_cfg: Optional[AutoBalanceConfig] = None
        self._bp_auto_balance_lock = asyncio.Lock()
        self._bp_auto_balance_cooldown_until: Optional[float] = None
        self._bp_auto_balance_status: Dict[str, Any] = {
            "enabled": False,
            "cooldown_until": None,
            "cooldown_active": False,
            "measurement": None,
            "last_error": None,
            "last_request_id": None,
            "last_action_at": None,
            "last_transfer_amount": None,
            "last_direction": None,
            "pending": False,
        }
        self._bp_auto_balance_task: Optional[asyncio.Task] = None
        # PARA adjustment history should stick around longer than the default
        # so the dashboard doesn't “forget” after an hour.
        self._para_adjustments = GrvtAdjustmentManager(
            history_limit=20,
            retention_seconds=3 * 24 * 3600,
        )

        # Backpack volume booster state (multi-runner, keyed by symbol).
        self._bp_volume = BackpackVolumeMultiState()

        # Persisted per-symbol history for the Backpack volume panel.
        # Shape: {"SYMBOL": {"updated_at": ms, "summary": {...}, "recent": [...]}}
        self._bp_volume_history: Dict[str, Dict[str, Any]] = {}
        self._load_bp_volume_history()

        # Backpack WS L1 cache (best-effort; falls back to HTTP when unavailable).
        self._bp_bbo_ws = None
        if BackpackBookTickerWS is not None:
            with suppress(Exception):
                self._bp_bbo_ws = BackpackBookTickerWS()

        # Load persisted auto balance configs (if any)
        self._load_persisted_para_auto_balance_config()
        self._load_persisted_bp_auto_balance_config()
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
                web.get("/para/auto_balance/config", self.handle_para_auto_balance_get),
                web.post("/para/auto_balance/config", self.handle_para_auto_balance_update),
                web.get("/backpack/auto_balance/config", self.handle_bp_auto_balance_get),
                web.post("/backpack/auto_balance/config", self.handle_bp_auto_balance_update),
                web.get("/risk_alert/settings", self.handle_risk_alert_settings),
                web.post("/risk_alert/settings", self.handle_risk_alert_update),
                web.get("/para/risk_alert/settings", self.handle_para_risk_alert_settings),
                web.post("/para/risk_alert/settings", self.handle_para_risk_alert_update),
                web.get("/bp/risk_alert/settings", self.handle_bp_risk_alert_settings),
                web.post("/bp/risk_alert/settings", self.handle_bp_risk_alert_update),
                web.get("/risk_alert/history", self.handle_risk_alert_history),
                web.post("/risk_alert/test", self.handle_risk_alert_test),
                web.get("/grvt/adjustments", self.handle_grvt_adjustments),
                web.post("/grvt/adjust", self.handle_grvt_adjust),
                web.post("/grvt/transfer", self.handle_grvt_transfer),
                web.post("/grvt/adjust/ack", self.handle_grvt_adjust_ack),
                web.get("/para/adjustments", self.handle_para_adjustments),
                web.post("/para/adjust", self.handle_para_adjust),
                web.post("/para/transfer", self.handle_para_transfer),
                web.post("/para/adjust/ack", self.handle_para_adjust_ack),

                # Backpack adjustments (monitor-executed)
                web.get("/backpack/adjustments", self.handle_backpack_adjustments_list),
                web.post("/backpack/adjustments/clear", self.handle_backpack_adjustments_clear),
                web.post("/backpack/adjust", self.handle_backpack_adjust_create),
                web.post("/backpack/adjust/ack", self.handle_backpack_adjust_ack),

                # Backpack volume booster
                web.get("/api/backpack/volume/markets", self.handle_bp_volume_markets),
                web.post("/api/backpack/volume/start", self.handle_bp_volume_start),
                web.post("/api/backpack/volume/stop", self.handle_bp_volume_stop),
                web.get("/api/backpack/volume/status", self.handle_bp_volume_status),
                web.get("/api/backpack/volume/metrics", self.handle_bp_volume_metrics),
                web.post("/api/backpack/volume/history/clear", self.handle_bp_volume_history_clear),

                # Backpack BBO preview (L1 bid/ask + capacity + estimated wear)
                web.get("/api/backpack/bbo", self.handle_bp_bbo_preview),
            ]
        )
        self._app.on_startup.append(self._on_startup)
        self._app.on_cleanup.append(self._on_cleanup)

    def _load_persisted_para_auto_balance_config(self) -> None:
        path = PERSISTED_PARA_AUTO_BALANCE_FILE
        if not path.exists():
            return
        try:
            raw = path.read_text(encoding="utf-8")
            payload = json.loads(raw)
        except Exception as exc:
            LOGGER.warning("Failed to load persisted PARA auto balance config: %s", exc)
            return
        if not isinstance(payload, dict):
            return
        enabled = payload.get("enabled")
        if enabled is False:
            return
        config_block = payload.get("config")
        if not isinstance(config_block, dict):
            return
        try:
            cfg = self._parse_auto_balance_config(config_block, default_currency="USDC")
        except Exception as exc:
            LOGGER.warning("Ignoring persisted PARA auto balance config (invalid): %s", exc)
            return
        self._update_para_auto_balance_config(cfg)
        LOGGER.info("Loaded persisted PARA auto balance config from disk")

    def _persist_para_auto_balance_config(self) -> None:
        path = PERSISTED_PARA_AUTO_BALANCE_FILE
        try:
            payload = {
                "enabled": bool(self._para_auto_balance_cfg),
                "config": self._para_auto_balance_config_as_payload(),
                "updated_at": time.time(),
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            LOGGER.warning("Failed to persist PARA auto balance config: %s", exc)

    @property
    def app(self) -> web.Application:
        return self._app

    async def _on_startup(self, app: web.Application) -> None:
        del app  # unused
        if self._bp_bbo_ws is not None:
            with suppress(Exception):
                await self._bp_bbo_ws.start()
        await self._coordinator.start_background_tasks()

    async def _on_cleanup(self, app: web.Application) -> None:
        del app  # unused
        # volatility monitor removed
        await self._coordinator.stop_background_tasks()

        if self._bp_bbo_ws is not None:
            with suppress(Exception):
                await self._bp_bbo_ws.stop()

        # Stop backpack volume task if running.
        tasks: List[asyncio.Task] = []
        async with self._bp_volume.lock:
            for sym, st in list(self._bp_volume.states.items()):
                st.running = False
                if st.task:
                    tasks.append(st.task)
                    st.task = None
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(Exception):
                await task

        # Best-effort persist.
        with suppress(Exception):
            self._save_bp_volume_history()

    def _load_bp_volume_history(self) -> None:
        try:
            if not BP_VOLUME_HISTORY_FILE.exists():
                self._bp_volume_history = {}
                return
            raw = BP_VOLUME_HISTORY_FILE.read_text(encoding="utf-8")
            payload = json.loads(raw)
            if isinstance(payload, dict):
                self._bp_volume_history = {str(k).upper(): v for k, v in payload.items() if isinstance(v, dict)}
            else:
                self._bp_volume_history = {}
        except Exception:
            self._bp_volume_history = {}

    def _save_bp_volume_history(self) -> None:
        try:
            BP_VOLUME_HISTORY_FILE.write_text(
                json.dumps(self._bp_volume_history, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _persist_bp_volume_snapshot(self, symbol: str, summary: Dict[str, Any], recent: List[Dict[str, Any]]) -> None:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return
        self._bp_volume_history[sym] = {
            "symbol": sym,
            "updated_at": int(time.time() * 1000),
            "summary": summary,
            "recent": (recent or [])[:200],
        }
        with suppress(Exception):
            self._save_bp_volume_history()

    def _get_bp_volume_history_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return None
        snap = self._bp_volume_history.get(sym)
        return snap if isinstance(snap, dict) else None

    async def handle_bp_volume_markets(self, request: web.Request) -> web.Response:
        """List available Backpack PERP markets (e.g. ETH-PERP)."""
        if _coord_debug_enabled():
            LOGGER.info(
                "[debug] bp_volume_markets request: remote=%s method=%s path=%s query=%s",
                request.remote,
                request.method,
                request.path,
                dict(request.query),
            )
        if BackpackClient is None:
            payload: Dict[str, Any] = {
                "ok": False,
                "error": "Backpack dependencies not available",
            }
            if _coord_debug_enabled():
                payload["debug"] = {
                    "BackpackClient": "ok" if BackpackClient is not None else "None",
                    "import_error": _BACKPACK_IMPORT_ERROR,
                    "sys_executable": sys.executable,
                    "repo_root": str(REPO_ROOT),
                }
                LOGGER.warning(
                    "[debug] Backpack dependencies missing: BackpackClient=%s err=%s",
                    payload["debug"]["BackpackClient"],
                    payload["debug"]["import_error"],
                )
            return web.json_response(payload, status=500)

        try:
            # Important: listing markets should NOT require account credentials.
            # BackpackClient enforces BACKPACK_PUBLIC_KEY/BACKPACK_SECRET_KEY in _validate_config.
            # Use the official SDK Public client directly.
            try:
                from bpx.public import Public  # type: ignore
            except Exception as exc:
                raise RuntimeError(f"bpx_public_import_failed: {exc}")

            public_client = Public()
            markets = public_client.get_markets()
            if _coord_debug_enabled():
                LOGGER.info(
                    "[debug] markets payload type=%s len=%s",
                    type(markets).__name__,
                    (len(markets) if isinstance(markets, list) else "-"),
                )
            out: List[str] = []
            if isinstance(markets, list):
                for market in markets:
                    if not isinstance(market, dict):
                        continue
                    if str(market.get("marketType") or "").upper() != "PERP":
                        continue
                    symbol = str(market.get("symbol") or "").strip()
                    if symbol:
                        out.append(symbol)
            out = sorted(set(out))
            return web.json_response({"ok": True, "markets": out})
        except Exception as exc:
            LOGGER.exception("bp_volume_markets failed: %s: %s", type(exc).__name__, exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    async def handle_bp_volume_status(self, request: web.Request) -> web.Response:
        del request
        env_diag = _bp_env_diagnostics()
        async with self._bp_volume.lock:
            runs = []
            for sym, state in sorted(self._bp_volume.states.items()):
                task_done = None
                task_cancelled = None
                task_exception = None
                if getattr(state, "task", None) is not None:
                    with suppress(Exception):
                        task_done = bool(state.task.done())  # type: ignore[union-attr]
                    with suppress(Exception):
                        task_cancelled = bool(state.task.cancelled())  # type: ignore[union-attr]
                    with suppress(Exception):
                        if state.task.done():  # type: ignore[union-attr]
                            exc = state.task.exception()  # type: ignore[union-attr]
                            if exc is not None:
                                task_exception = f"{type(exc).__name__}: {exc}"
                summary_dict: Dict[str, Any] = {}
                with suppress(Exception):
                    summary_dict = state.summary.to_dict(state.cfg) or {}
                quote = summary_dict.get("volume_quote")
                wear_total_net = summary_dict.get("wear_total_net")
                wear_per_10k = summary_dict.get("wear_total_net_per_10k")
                runs.append(
                    {
                        "symbol": sym,
                        "running": bool(state.running),
                        "run_id": state.run_id,
                        "started_at": state.started_at,
                        "cycles_done": state.summary.cycles_done,
                        "cycles_target": state.cfg.cycles,
                        "last_error": state.last_error,
                        "last_note": getattr(state, "last_note", ""),
                        "last_gate": getattr(state, "last_gate", ""),
                        "last_bid1": getattr(state, "last_bid1", ""),
                        "last_ask1": getattr(state, "last_ask1", ""),
                        "last_cap_qty": getattr(state, "last_cap_qty", ""),
                        "last_spread_bps": getattr(state, "last_spread_bps", None),
                        "last_qty_exec": getattr(state, "last_qty_exec", ""),
                        "last_heartbeat_ts": getattr(state, "last_heartbeat_ts", 0.0),
                        "last_cycle_attempt_ts": getattr(state, "last_cycle_attempt_ts", 0.0),
                        "task_done": task_done,
                        "task_cancelled": task_cancelled,
                        "task_exception": task_exception or getattr(state, "task_last_exception", ""),
                        "volume_quote": quote,
                        "wear_total_net": wear_total_net,
                        "wear_total_net_per_10k": wear_per_10k,
                    }
                )
            return web.json_response({"ok": True, "env": env_diag, "runs": runs})

    async def handle_bp_volume_metrics(self, request: web.Request) -> web.Response:
        # Note: this endpoint historically returned the *in-memory* state.
        # To make "clear" survive refresh (and keep showing the cleared state),
        # when the runner is NOT active we prefer returning the persisted snapshot.
        symbol_q = str(request.query.get("symbol") or "").strip().upper()
        live_state: Optional[BackpackVolumeState] = None
        live_symbol: str = ""
        async with self._bp_volume.lock:
            if symbol_q:
                live_state = self._bp_volume.states.get(symbol_q)
                live_symbol = symbol_q
            elif self._bp_volume.states:
                # Back-compat: if no symbol given, pick the first running one.
                for sym, st in self._bp_volume.states.items():
                    if st.running:
                        live_state = st
                        live_symbol = sym
                        break
                if live_state is None:
                    # otherwise just pick any
                    sym, st = next(iter(self._bp_volume.states.items()))
                    live_state = st
                    live_symbol = sym

        if live_state is not None and live_symbol:
            summary = live_state.summary.to_dict(live_state.cfg)
            recent = [rec.to_dict() for rec in list(live_state.recent)]
            running = bool(live_state.running)
            if running:
                self._persist_bp_volume_snapshot(live_symbol, summary, recent)
                return web.json_response({"ok": True, "summary": summary, "recent": recent, "symbol": live_symbol, "source": "memory"})

        # Runner not active (or no live state): STRICTLY return persisted snapshot (or empty).
        pick_symbol = symbol_q or live_symbol
        if not pick_symbol:
            return web.json_response({"ok": True, "summary": {}, "recent": [], "symbol": None, "source": "disk", "updated_at": None})
        snap = self._get_bp_volume_history_snapshot(pick_symbol)
        if not snap:
            return web.json_response({"ok": True, "summary": {}, "recent": [], "symbol": pick_symbol, "source": "disk", "updated_at": None})
        return web.json_response(
            {
                "ok": True,
                "summary": snap.get("summary") or {},
                "recent": snap.get("recent") or [],
                "symbol": pick_symbol,
                "source": "disk",
                "updated_at": snap.get("updated_at"),
            }
        )

    async def handle_bp_volume_history_clear(self, request: web.Request) -> web.Response:
        """Clear persisted history for a given symbol (survives refresh)."""
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        symbol = str(payload.get("symbol") or "").strip().upper()
        if not symbol:
            return web.json_response({"ok": False, "error": "symbol required"}, status=400)
        with suppress(Exception):
            self._bp_volume_history.pop(symbol, None)
            self._save_bp_volume_history()
        return web.json_response({"ok": True, "symbol": symbol})

    async def handle_bp_volume_start(self, request: web.Request) -> web.Response:
        if BackpackClient is None:
            return web.json_response({"ok": False, "error": "Backpack dependencies not available"}, status=500)

        env_diag = _bp_env_diagnostics()
        if not env_diag["configured"]:
            return web.json_response(
                {
                    "ok": False,
                    "error": "Backpack env not configured (missing required environment variables)",
                    "env": env_diag,
                },
                status=400,
            )

        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        symbol = str(payload.get("symbol") or "").strip()
        if not symbol:
            return web.json_response({"ok": False, "error": "symbol required"}, status=400)

        symbol_u = symbol.strip().upper()

        cfg = BackpackVolumeConfig(
            symbol=symbol,
            qty_per_cycle=_decimal(payload.get("qty_per_cycle"), "0"),
            cycles=int(payload.get("cycles") or 0),
            max_spread_bps=float(payload.get("max_spread_bps") or 0.0),
            cooldown_ms=int(payload.get("cooldown_ms") or 1500),
            cooldown_ms_min=int(payload.get("cooldown_ms_min") or 0),
            cooldown_ms_max=int(payload.get("cooldown_ms_max") or 0),
            depth_safety_factor=float(payload.get("depth_safety_factor") or 0.7),
            min_cap_qty=_decimal(payload.get("min_cap_qty"), "0"),
            fee_rate=float(payload.get("fee_rate") or 0.00026),
            rebate_rate=float(payload.get("rebate_rate") or 0.45),
        )
        cfg.net_fee_rate = cfg.fee_rate * max(0.0, 1.0 - cfg.rebate_rate)

        # Quantity precision: follow user's input decimals for qty_per_cycle.
        # e.g. 100 -> 0 decimals, 0.1 -> 1 decimal.
        with suppress(Exception):
            exp = cfg.qty_per_cycle.normalize().as_tuple().exponent
            cfg.qty_scale = max(0, -int(exp))

        if cfg.qty_per_cycle <= 0:
            return web.json_response({"ok": False, "error": "qty_per_cycle must be > 0"}, status=400)
        if cfg.max_spread_bps <= 0:
            return web.json_response({"ok": False, "error": "max_spread_bps must be > 0"}, status=400)

        async with self._bp_volume.lock:
            existing = self._bp_volume.states.get(symbol_u)
            if existing and existing.running and existing.task:
                return web.json_response({"ok": True, "running": True, "run_id": existing.run_id, "symbol": symbol_u})

            # Continue totals from persisted snapshot (if any) so stop/start does not reset totals.
            snap_summary: Dict[str, Any] = {}
            with suppress(Exception):
                snap = self._get_bp_volume_history_snapshot(symbol_u)
                if isinstance(snap, dict):
                    snap_summary = snap.get("summary") or {}
            base_summary = BackpackVolumeSummary.from_snapshot(snap_summary)

            state = BackpackVolumeState(
                running=True,
                run_id=uuid.uuid4().hex,
                started_at=_now_ts(),
                last_error="",
                last_note="",
                cfg=cfg,
                summary=base_summary,
                recent=deque(maxlen=200),
            )

            state.task = asyncio.create_task(self._bp_volume_runner(symbol_u, state.run_id))
            self._bp_volume.states[symbol_u] = state
            return web.json_response({"ok": True, "running": True, "run_id": state.run_id, "symbol": symbol_u})

    async def handle_bp_volume_stop(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        symbol = str(payload.get("symbol") or "").strip().upper()

        task: Optional[asyncio.Task] = None
        async with self._bp_volume.lock:
            if symbol:
                st = self._bp_volume.states.get(symbol)
                if st is not None:
                    st.running = False
                    task = st.task
                    st.task = None
            else:
                # Back-compat: stop all
                tasks: List[asyncio.Task] = []
                for sym, st in list(self._bp_volume.states.items()):
                    st.running = False
                    if st.task:
                        tasks.append(st.task)
                        st.task = None
                if tasks:
                    # cancel outside lock
                    task = None
        if symbol and task:
            task.cancel()
        if not symbol:
            # stop all (best-effort)
            tasks2: List[asyncio.Task] = []
            async with self._bp_volume.lock:
                for sym, st in list(self._bp_volume.states.items()):
                    if st.task:
                        tasks2.append(st.task)
                        st.task = None
            for t in tasks2:
                t.cancel()
        return web.json_response({"ok": True, "running": False, "symbol": symbol or None})

    @staticmethod
    def _estimate_backpack_wear(
        *,
        bid1: Decimal,
        ask1: Decimal,
        qty: Decimal,
        fee_rate: float,
        net_fee_rate: float,
    ) -> Dict[str, Any]:
        """Estimate wear for a BUY->SELL market cycle using only L1.

        We don't know the actual fill prices yet, so we approximate:
        - buy_avg ~= ask1
        - sell_avg ~= bid1

        Returns quote-denominated amounts.
        """
        if qty <= 0 or bid1 <= 0 or ask1 <= 0:
            return {
                "qty": str(qty),
                "buy_avg_est": str(Decimal("0")),
                "sell_avg_est": str(Decimal("0")),
                "volume_quote_est": str(Decimal("0")),
                "wear_spread_est": str(Decimal("0")),
                "fee_gross_est": str(Decimal("0")),
                "fee_net_est": str(Decimal("0")),
                "rebate_est": str(Decimal("0")),
                "wear_total_net_est": str(Decimal("0")),
                "wear_per_10k_est": None,
            }

        buy_avg = ask1
        sell_avg = bid1
        volume_quote = (buy_avg + sell_avg) * qty
        wear_spread = (buy_avg - sell_avg) * qty
        if wear_spread < 0:
            wear_spread = abs(wear_spread)

        fee_gross = volume_quote * Decimal(str(fee_rate))
        fee_net = volume_quote * Decimal(str(net_fee_rate))
        rebate = fee_gross - fee_net
        wear_total_net = wear_spread + fee_net

        wear_per_10k: Optional[float] = None
        try:
            if volume_quote > 0:
                wear_per_10k = float((wear_total_net / volume_quote) * Decimal("10000"))
        except Exception:
            wear_per_10k = None

        return {
            "qty": str(qty),
            "buy_avg_est": str(buy_avg),
            "sell_avg_est": str(sell_avg),
            "volume_quote_est": str(volume_quote),
            "wear_spread_est": str(wear_spread),
            "fee_gross_est": str(fee_gross),
            "fee_net_est": str(fee_net),
            "rebate_est": str(rebate),
            "wear_total_net_est": str(wear_total_net),
            "wear_per_10k_est": wear_per_10k,
        }

    async def handle_bp_bbo_preview(self, request: web.Request) -> web.Response:
        """Preview Backpack L1 (bid/ask + qty), spread, and estimated wear for a given symbol.

        Query params:
        - symbol: e.g. ETH-PERP (required)
        - qty: base qty to estimate wear for (optional; defaults to current cfg.qty_per_cycle)
        - depth_safety_factor: optional; defaults to current cfg.depth_safety_factor
        - fee_rate / rebate_rate: optional; defaults to current cfg values
        """
        if BackpackClient is None:
            return web.json_response({"ok": False, "error": "Backpack dependencies not available"}, status=500)

        params = request.rel_url.query
        symbol = str(params.get("symbol") or "").strip()
        if not symbol:
            return web.json_response({"ok": False, "error": "symbol required"}, status=400)

        symbol_u = symbol.strip().upper()
        async with self._bp_volume.lock:
            st = self._bp_volume.states.get(symbol_u)
            if st is not None:
                cfg = copy.deepcopy(st.cfg)
            else:
                # Not running: build a minimal default config for estimation.
                cfg = BackpackVolumeConfig(symbol=symbol_u)

        qty = _decimal(params.get("qty"), str(cfg.qty_per_cycle or "0"))
        fee_rate = float(params.get("fee_rate") or cfg.fee_rate)
        rebate_rate = float(params.get("rebate_rate") or cfg.rebate_rate)
        net_fee_rate = fee_rate * max(0.0, 1.0 - rebate_rate)
        depth_safety_factor = float(params.get("depth_safety_factor") or cfg.depth_safety_factor)

        source = "ws"
        age_ms: Optional[float] = None

        bid1: Decimal = Decimal("0")
        bid1_qty: Decimal = Decimal("0")
        ask1: Decimal = Decimal("0")
        ask1_qty: Decimal = Decimal("0")

        quote: Optional[Tuple[Decimal, Decimal, Decimal, Decimal]] = None
        if getattr(self, "_bp_bbo_ws", None) is not None:
            with suppress(Exception):
                quote = await self._bp_bbo_ws.get_quote(symbol)  # type: ignore[union-attr]
            if quote:
                last_ts = None
                with suppress(Exception):
                    last_ts = self._bp_bbo_ws.last_update_ts(symbol)  # type: ignore[union-attr]
                if last_ts:
                    age_ms = max(0.0, (time.time() - float(last_ts)) * 1000.0)
                bid1, bid1_qty, ask1, ask1_qty = quote

        # WS-only: if the symbol hasn't produced a quote yet, return a non-error
        # warm-up payload so the UI doesn't flash an error when switching symbols.
        if not quote:
            return web.json_response(
                {
                    "ok": True,
                    "ready": False,
                    "note": "waiting_ws_bbo",
                    "symbol": symbol,
                    "source": "ws",
                    "age_ms": age_ms,
                    "bid1": None,
                    "bid1_qty": None,
                    "ask1": None,
                    "ask1_qty": None,
                    "ts": _now_ts(),
                }
            )

        mid = (bid1 + ask1) / Decimal("2") if (bid1 > 0 and ask1 > 0) else Decimal("0")
        spread_abs = ask1 - bid1
        spread_bps = 0.0
        try:
            if mid > 0:
                spread_bps = float((spread_abs / mid) * Decimal("10000"))
        except Exception:
            spread_bps = 0.0

        cap_qty = min(bid1_qty, ask1_qty)
        safe_cap_qty = cap_qty * Decimal(str(max(0.0, min(depth_safety_factor, 1.0))))
        qty_exec = min(qty, safe_cap_qty)

        wear = self._estimate_backpack_wear(
            bid1=bid1,
            ask1=ask1,
            qty=qty_exec,
            fee_rate=fee_rate,
            net_fee_rate=net_fee_rate,
        )

        return web.json_response(
            {
                "ok": True,
                "ready": True,
                "symbol": symbol,
                "source": source,
                "age_ms": age_ms,
                "bid1": str(bid1),
                "bid1_qty": str(bid1_qty),
                "ask1": str(ask1),
                "ask1_qty": str(ask1_qty),
                "spread_abs": str(spread_abs),
                "spread_bps": spread_bps,
                "cap_qty": str(cap_qty),
                "depth_safety_factor": depth_safety_factor,
                "safe_cap_qty": str(safe_cap_qty),
                "qty_input": str(qty),
                "qty_exec": str(qty_exec),
                "fee_rate": fee_rate,
                "rebate_rate": rebate_rate,
                "net_fee_rate": net_fee_rate,
                "wear_est": wear,
                "ts": _now_ts(),
            }
        )

    async def _bp_volume_runner(self, symbol_u: str, run_id: str) -> None:
        """Background task for Backpack volume boosting (BUY market -> SELL market)."""
        # Per-symbol runner; stop when state.running flips or run_id changes.
        # Defensive: if Backpack deps are missing, don't crash the task with AssertionError.
        if BackpackClient is None:
            async with self._bp_volume.lock:
                state = self._bp_volume.states.get(symbol_u)
                if state is not None and state.run_id == run_id:
                    state.last_error = "cycle_failed: Backpack dependencies not available"
                    state.running = False
            return

        # IMPORTANT: WS-only for BBO. We still use BackpackClient for order placement,
        # but we must NOT use it for best bid/ask discovery.
        client = _make_backpack_client()

        consecutive_failures = 0
        last_cycle_at: float = 0.0

        while True:
            async with self._bp_volume.lock:
                state = self._bp_volume.states.get(symbol_u)
                if state is None:
                    return
                if (not state.running) or state.run_id != run_id:
                    return
                cfg = copy.deepcopy(state.cfg)
                with suppress(Exception):
                    state.last_heartbeat_ts = _now_ts()

            # Defensive: symbol must be a non-empty string. If it's None/empty,
            # fail fast so we don't raise confusing AttributeError deep in client code (e.g. `.upper()`).
            symbol = str(getattr(cfg, "symbol", "") or "").strip()
            if not symbol:
                async with self._bp_volume.lock:
                    state = self._bp_volume.states.get(symbol_u)
                    if state is not None and state.run_id == run_id:
                        state.last_error = "cycle_failed: invalid symbol"
                        state.running = False
                return

            # Pre-flight: if last cycle left a residual position, flatten it first.
            # User assumption: market orders fill immediately; we still guard with best-effort checks.
            try:
                if hasattr(client, "get_account_positions"):
                    pos_abs = await client.get_account_positions()  # type: ignore[attr-defined]
                    if isinstance(pos_abs, Decimal) and pos_abs > 0:
                        # Best-effort: attempt both directions; only the correct one will reduce exposure.
                        for side in ("sell", "buy"):
                            with suppress(Exception):
                                await client.place_market_order(symbol, pos_abs, side)
            except Exception:
                # Never block the runner on position checks.
                pass

            now = _now_ts()

            # Cooldown scheduling:
            # - If cooldown_ms_min/max are set (max>0 and max>=min), pick a random cooldown each cycle.
            # - Otherwise use fixed cooldown_ms.
            cooldown_s = 0.0
            try:
                mn = int(getattr(cfg, "cooldown_ms_min", 0) or 0)
                mx = int(getattr(cfg, "cooldown_ms_max", 0) or 0)
                if mx > 0:
                    if mn < 0:
                        mn = 0
                    if mx < mn:
                        mx = mn
                    cooldown_ms = random.randint(mn, mx)
                    cooldown_s = max(0.0, float(cooldown_ms) / 1000.0)
                else:
                    cooldown_s = max(0.0, float(cfg.cooldown_ms) / 1000.0)
            except Exception:
                cooldown_s = max(0.0, float(cfg.cooldown_ms) / 1000.0)
            if last_cycle_at and (now - last_cycle_at) < cooldown_s:
                await asyncio.sleep(min(0.25, cooldown_s))
                continue

            # Fetch L1 (WS-only).
            quote: Optional[Tuple[Decimal, Decimal, Decimal, Decimal]] = None
            if getattr(self, "_bp_bbo_ws", None) is not None:
                with suppress(Exception):
                    quote = await self._bp_bbo_ws.get_quote(symbol)  # type: ignore[union-attr]
            if not quote:
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        st.last_gate = "bbo_ws_warming_up"
                        st.last_note = f"waiting_ws_bbo: symbol={symbol}"
                await asyncio.sleep(0.25)
                continue

            bid1, bid1_qty, ask1, ask1_qty = quote

            if bid1 <= 0 or ask1 <= 0:
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        st.last_gate = "invalid_l1"
                        st.last_bid1 = str(bid1)
                        st.last_ask1 = str(ask1)
                await asyncio.sleep(0.5)
                continue

            mid = (bid1 + ask1) / Decimal("2")
            spread_abs = ask1 - bid1
            spread_bps = 0.0
            try:
                if mid > 0:
                    spread_bps = float((spread_abs / mid) * Decimal("10000"))
            except Exception:
                spread_bps = 0.0

            cap_qty = min(bid1_qty, ask1_qty)
            safe_qty = cap_qty * Decimal(str(max(0.0, min(cfg.depth_safety_factor, 1.0))))
            qty_exec = min(cfg.qty_per_cycle, safe_qty)

            # Save last snapshot for UI/debug.
            async with self._bp_volume.lock:
                st = self._bp_volume.states.get(symbol_u)
                if st is not None and st.run_id == run_id:
                    st.last_bid1 = str(bid1)
                    st.last_ask1 = str(ask1)
                    st.last_cap_qty = str(cap_qty)
                    st.last_spread_bps = spread_bps
                    st.last_qty_exec = str(qty_exec)

            # Depth gate: only execute when current L1 capacity can cover BOTH legs comfortably.
            # Requirement: cap_qty must be >= 2 * qty_per_cycle, otherwise wait.
            if cap_qty < (cfg.qty_per_cycle * Decimal("2")):
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        # throttle updates: only update message occasionally
                        if not st.last_note.startswith("waiting_depth") or (now % 5) < 1:
                            st.last_note = (
                                f"waiting_depth: cap_qty={cap_qty} < 2*qty_per_cycle={cfg.qty_per_cycle * Decimal('2')} "
                                f"| symbol={symbol}"
                            )
                        st.last_gate = "waiting_depth"
                await asyncio.sleep(0.25)
                continue

            # Constrain quantity decimals to match user's input (avoid BP: Quantity decimal too long).
            # Example: qty_per_cycle=100 -> scale 0, qty_per_cycle=0.1 -> scale 1.
            try:
                scale = int(getattr(cfg, "qty_scale", 0) or 0)
                if scale <= 0:
                    qty_exec = qty_exec.quantize(Decimal("1"))
                else:
                    qty_exec = qty_exec.quantize(Decimal("1").scaleb(-scale))
            except Exception:
                pass

            if cfg.min_cap_qty > 0 and cap_qty < cfg.min_cap_qty:
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        st.last_gate = "min_cap_qty"
                await asyncio.sleep(0.25)
                continue
            if spread_bps <= 0 or spread_bps > cfg.max_spread_bps:
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        st.last_gate = "spread"
                await asyncio.sleep(0.25)
                continue
            if qty_exec <= 0:
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        st.last_gate = "qty_exec"
                await asyncio.sleep(0.25)
                continue

            # Execute BUY -> SELL (market), sell qty follows buy filled.
            try:
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        st.last_cycle_attempt_ts = _now_ts()
                # User assumption: both legs fill immediately. Run them concurrently to reduce serial latency.
                buy, sell = await asyncio.gather(
                    client.place_market_order(symbol, qty_exec, "buy"),
                    client.place_market_order(symbol, qty_exec, "sell"),
                )

                if not buy.success:
                    raise RuntimeError(buy.error_message or "buy failed")
                if not sell.success:
                    raise RuntimeError(sell.error_message or "sell failed")

                buy_filled_qty = buy.filled_size if buy.filled_size is not None else (buy.size or Decimal("0"))
                buy_avg = buy.price or Decimal("0")
                sell_filled_qty = sell.filled_size if sell.filled_size is not None else (sell.size or Decimal("0"))
                sell_avg = sell.price or Decimal("0")

                if buy_filled_qty <= 0 or buy_avg <= 0:
                    raise RuntimeError("buy missing avg/filled")
                if sell_filled_qty <= 0 or sell_avg <= 0:
                    raise RuntimeError("sell missing avg/filled")

            except Exception as exc:
                consecutive_failures += 1
                tb = ""
                try:
                    import traceback

                    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                    # avoid giant payloads in UI
                    tb = tb[-2000:]
                except Exception:
                    tb = ""
                async with self._bp_volume.lock:
                    st = self._bp_volume.states.get(symbol_u)
                    if st is not None and st.run_id == run_id:
                        ctx = f"symbol={symbol} qty_exec={qty_exec} spread_bps={spread_bps:.2f}"
                        st.last_error = f"cycle_failed: {exc} | {ctx}{(' | tb=' + tb) if tb else ''}"
                        st.last_gate = "cycle_failed"
                        if consecutive_failures >= 3:
                            st.running = False
                await asyncio.sleep(0.5)
                continue

            consecutive_failures = 0
            last_cycle_at = _now_ts()

            async with self._bp_volume.lock:
                st = self._bp_volume.states.get(symbol_u)
                if st is not None and st.run_id == run_id:
                    st.last_gate = "executed"

            paired_qty = min(buy_filled_qty, sell_filled_qty)
            buy_notional = buy_avg * buy_filled_qty
            sell_notional = sell_avg * sell_filled_qty
            volume_quote = buy_notional + sell_notional
            volume_base = buy_filled_qty + sell_filled_qty

            wear_spread = (buy_avg - sell_avg) * paired_qty
            if wear_spread < 0:
                wear_spread = abs(wear_spread)

            fee_gross = volume_quote * Decimal(str(cfg.fee_rate))
            fee_net = volume_quote * Decimal(str(cfg.net_fee_rate))
            wear_total_net = wear_spread + fee_net

            rec = BackpackVolumeCycleRecord(
                ts=_now_ts(),
                symbol=symbol,
                bid1=bid1,
                ask1=ask1,
                bid1_qty=bid1_qty,
                ask1_qty=ask1_qty,
                spread_bps=spread_bps,
                qty_exec=qty_exec,
                buy_avg=buy_avg,
                sell_avg=sell_avg,
                paired_qty=paired_qty,
                wear_spread=wear_spread,
                fee_gross=fee_gross,
                fee_net=fee_net,
                wear_total_net=wear_total_net,
            )

            async with self._bp_volume.lock:
                st = self._bp_volume.states.get(symbol_u)
                if st is None or st.run_id != run_id:
                    return
                st.recent.appendleft(rec)
                st.summary.cycles_done += 1
                st.summary.volume_base_total += volume_base
                st.summary.volume_quote_total += volume_quote
                st.summary.wear_spread_total += wear_spread
                st.summary.fee_total_gross += fee_gross
                st.summary.fee_total_net += fee_net
                st.summary.rebate_total += (fee_gross - fee_net)

                # Stop if reached target cycles (non-zero).
                if st.cfg.cycles > 0 and st.summary.cycles_done >= st.cfg.cycles:
                    st.running = False
                    return

            # Small yield.
            await asyncio.sleep(0)

            

    async def handle_dashboard_redirect(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request, redirect_on_fail=True)
        raise web.HTTPFound("/dashboard")

    async def handle_dashboard(self, request: web.Request) -> web.StreamResponse:
        self._enforce_dashboard_auth(request, redirect_on_fail=True)
        if not DASHBOARD_PATH.exists():
            raise web.HTTPNotFound(text="dashboard asset missing; ensure hedge_dashboard.html exists")
        # IMPORTANT: avoid stale cached dashboard on VPS/browser/proxy.
        # We also inject a small build marker so operators can confirm the
        # currently served HTML version.
        try:
            raw = DASHBOARD_PATH.read_text(encoding="utf-8")
        except Exception:
            return web.FileResponse(path=DASHBOARD_PATH)

        build_id = f"{SERVER_INSTANCE_ID}:{int(time.time())}"
        marker = (
            "\n<!-- hedge-dashboard-build:{build_id} -->\n"
            "<script>window.__HEDGE_DASHBOARD_BUILD = {json};</script>\n"
        ).format(build_id=build_id, json=json.dumps(build_id))
        if "</body>" in raw:
            body = raw.replace("</body>", marker + "</body>")
        else:
            body = raw + marker

        resp = web.Response(text=body, content_type="text/html")
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    async def handle_metrics(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload = await self._coordinator.snapshot()
        payload["grvt_adjustments"] = await self._adjustments.summary()
        payload["para_adjustments"] = await self._para_adjustments.summary()
        payload["auto_balance"] = self._auto_balance_status_snapshot()
        payload["bp_auto_balance"] = self._bp_auto_balance_status_snapshot()
        return web.json_response(payload)

    def _build_range_payload(self, points: Sequence[Mapping[str, float]]) -> Optional[Dict[str, Any]]:
        if not points:
            return None
        start_ts = points[0].get("ts")
        end_ts = points[-1].get("ts")
        if start_ts is None or end_ts is None:
            return None
        try:
            start_value = float(start_ts)
            end_value = float(end_ts)
        except (TypeError, ValueError):
            return None
        return {
            "start_ts": start_value,
            "end_ts": end_value,
            "start_iso": self._format_iso_timestamp(start_value),
            "end_iso": self._format_iso_timestamp(end_value),
        }

    @staticmethod
    def _format_iso_timestamp(value: Optional[float]) -> Optional[str]:
        if value is None:
            return None
        try:
            ts = float(value)
        except (TypeError, ValueError):
            return None
        if ts <= 0:
            return None
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (OSError, OverflowError, ValueError):
            return None

    def _raise_price_fetch_error(self, exc: Exception, symbol: str) -> None:
        message = str(exc)
        LOGGER.warning("Failed to download simulation history for %s: %s", symbol, message)
        if "-1121" in message or "Invalid symbol" in message:
            raise web.HTTPBadRequest(text=f"Binance 不识别交易对 {symbol}")
        raise web.HTTPBadGateway(text="failed to download price history")

    async def handle_update(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="update payload must be JSON")

        # ...existing code...
        # NOTE: BTC/ETH volatility + pair simulation endpoints were removed; this handler
        # continues to accept monitor updates for the dashboard.
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="update payload must be an object")

        await self._coordinator.update(body)
        snapshot = await self._coordinator.snapshot()
        snapshot["grvt_adjustments"] = await self._adjustments.summary()
        snapshot["para_adjustments"] = await self._para_adjustments.summary()
        snapshot["auto_balance"] = self._auto_balance_status_snapshot()
        snapshot["para_auto_balance"] = self._para_auto_balance_status_snapshot()
        snapshot["bp_auto_balance"] = self._bp_auto_balance_status_snapshot()

        # Trigger backpack auto balance evaluation in the background.
        # We intentionally do this post-update so it uses the freshest snapshot.
        with suppress(Exception):
            await self._maybe_bp_auto_balance(snapshot)
        return web.json_response(snapshot)

    # -----------------------------
    # Backpack auto balance
    # -----------------------------

    def _bp_auto_balance_config_as_payload(self) -> Optional[Dict[str, Any]]:
        return self._auto_balance_config_as_payload(self._bp_auto_balance_cfg)

    def _bp_auto_balance_status_snapshot(self) -> Dict[str, Any]:
        snapshot = copy.deepcopy(self._bp_auto_balance_status)
        snapshot["enabled"] = bool(self._bp_auto_balance_cfg)
        snapshot["config"] = self._bp_auto_balance_config_as_payload()
        snapshot["pending"] = bool(self._bp_auto_balance_task)
        return snapshot

    def _update_bp_auto_balance_config(self, config: Optional[AutoBalanceConfig]) -> None:
        self._bp_auto_balance_cfg = config
        self._bp_auto_balance_cooldown_until = None
        self._bp_auto_balance_status["enabled"] = bool(config)
        self._bp_auto_balance_status["config"] = self._bp_auto_balance_config_as_payload()
        self._bp_auto_balance_status["cooldown_until"] = None
        self._bp_auto_balance_status["cooldown_active"] = False
        self._bp_auto_balance_status["measurement"] = None
        self._bp_auto_balance_status["last_error"] = None
        self._bp_auto_balance_status["last_request_id"] = None
        self._bp_auto_balance_status["last_action_at"] = None
        self._bp_auto_balance_status["last_transfer_amount"] = None
        self._bp_auto_balance_status["last_direction"] = None
        self._bp_auto_balance_status["pending"] = False
        self._bp_auto_balance_task = None
        if not config:
            LOGGER.info("Backpack auto balance disabled via dashboard")
        else:
            LOGGER.info(
                "Backpack auto balance configured via dashboard: %s ↔ %s (threshold %.2f%%, min %s %s)",
                config.agent_a,
                config.agent_b,
                config.threshold_ratio * 100,
                self._decimal_to_str(config.min_transfer),
                config.currency,
            )

    def _load_persisted_bp_auto_balance_config(self) -> None:
        path = PERSISTED_BP_AUTO_BALANCE_FILE
        raw = _load_json_file(path, default=None)
        if not isinstance(raw, dict) or not raw:
            return
        enabled = bool(raw.get("enabled"))
        cfg_raw = raw.get("config")
        if not enabled:
            self._update_bp_auto_balance_config(None)
            return
        if not isinstance(cfg_raw, dict):
            return
        try:
            cfg = self._parse_auto_balance_config(cfg_raw, default_currency="USDC")
        except Exception:
            return
        self._update_bp_auto_balance_config(cfg)

    def _persist_bp_auto_balance_config(self) -> None:
        path = PERSISTED_BP_AUTO_BALANCE_FILE
        payload = {
            "enabled": bool(self._bp_auto_balance_cfg),
            "config": self._bp_auto_balance_config_as_payload(),
        }
        _atomic_write_json(path, payload)

    async def handle_bp_auto_balance_get(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        return web.json_response({
            "config": self._bp_auto_balance_config_as_payload(),
            "status": self._bp_auto_balance_status_snapshot(),
        })

    async def handle_bp_auto_balance_update(self, request: web.Request) -> web.Response:
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
            self._update_bp_auto_balance_config(None)
            self._persist_bp_auto_balance_config()
            return web.json_response({
                "config": None,
                "status": self._bp_auto_balance_status_snapshot(),
            })

        try:
            config = self._parse_auto_balance_config(body, default_currency="USDC")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))

        self._update_bp_auto_balance_config(config)
        self._persist_bp_auto_balance_config()
        return web.json_response({
            "config": self._bp_auto_balance_config_as_payload(),
            "status": self._bp_auto_balance_status_snapshot(),
        })

    def _extract_bp_equity_from_agent(self, agent_payload: Dict[str, Any], *, prefer_available: bool) -> Optional[Decimal]:
        bp_block = agent_payload.get("backpack_accounts")
        if not isinstance(bp_block, dict):
            return None
        summary = bp_block.get("summary")
        summary_block = summary if isinstance(summary, dict) else None
        if summary_block is None:
            return None
        # Backpack monitors currently report equity information primarily under
        # summary.collateral, e.g. netEquity / netEquityAvailable.
        collateral = summary_block.get("collateral")
        collateral_block = collateral if isinstance(collateral, dict) else None
        if collateral_block is not None:
            preferred = (
                ("netEquityAvailable", "netEquity", "assetsValue")
                if prefer_available
                else ("netEquity", "netEquityAvailable", "assetsValue")
            )
            for field in preferred:
                value = HedgeCoordinator._decimal_from(collateral_block.get(field))
                if value is not None:
                    return value

        # Fallback: accept any legacy summary-level fields.
        fields_available = ("available_equity", "available_balance", "equity", "balance")
        fields_equity_first = ("equity", "balance", "available_equity", "available_balance")
        fields = fields_available if prefer_available else fields_equity_first
        for field in fields:
            value = HedgeCoordinator._decimal_from(summary_block.get(field))
            if value is not None:
                return value

        # Final fallback: some payloads also include an accounts list with equity.
        accounts = bp_block.get("accounts")
        if isinstance(accounts, list) and accounts:
            first = accounts[0]
            if isinstance(first, dict):
                fields_accounts_available = ("available_equity", "available_balance", "equity", "balance")
                fields_accounts_equity_first = ("equity", "balance", "available_equity", "available_balance")
                fields_accounts = fields_accounts_available if prefer_available else fields_accounts_equity_first
                for field in fields_accounts:
                    value = HedgeCoordinator._decimal_from(first.get(field))
                    if value is not None:
                        return value
        return None

    def _compute_bp_auto_balance_measurement(self, snapshot: Dict[str, Any]) -> Optional[AutoBalanceMeasurement]:
        if not self._bp_auto_balance_cfg:
            return None
        agents_block = snapshot.get("agents")
        if not isinstance(agents_block, dict):
            return None
        entry_a = agents_block.get(self._bp_auto_balance_cfg.agent_a)
        entry_b = agents_block.get(self._bp_auto_balance_cfg.agent_b)
        if not isinstance(entry_a, dict) or not isinstance(entry_b, dict):
            return None
        equity_a = self._extract_bp_equity_from_agent(entry_a, prefer_available=self._bp_auto_balance_cfg.use_available_equity)
        equity_b = self._extract_bp_equity_from_agent(entry_b, prefer_available=self._bp_auto_balance_cfg.use_available_equity)
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
        source_agent = self._bp_auto_balance_cfg.agent_a if equity_a >= equity_b else self._bp_auto_balance_cfg.agent_b
        target_agent = self._bp_auto_balance_cfg.agent_b if source_agent == self._bp_auto_balance_cfg.agent_a else self._bp_auto_balance_cfg.agent_a
        transfer_amount = difference / Decimal("2")
        if transfer_amount <= 0:
            return None
        return AutoBalanceMeasurement(
            agent_a=self._bp_auto_balance_cfg.agent_a,
            agent_b=self._bp_auto_balance_cfg.agent_b,
            equity_a=equity_a,
            equity_b=equity_b,
            difference=difference,
            ratio=ratio,
            source_agent=source_agent,
            target_agent=target_agent,
            transfer_amount=transfer_amount,
        )

    async def _run_bp_auto_balance_transfer(
        self,
        *,
        measurement: AutoBalanceMeasurement,
        cfg: AutoBalanceConfig,
        requested_amount: Decimal,
    ) -> None:
        now = time.time()
        try:
            # Resolve destination internal transfer address from the latest coordinator snapshot.
            # The Backpack monitor requires a concrete address (or BACKPACK_INTERNAL_TRANSFER_PEER_ADDRESS).
            dest_address: Optional[str] = None
            with suppress(Exception):
                snap = await self._coordinator.snapshot()
                agents_block = snap.get("agents")
                if isinstance(agents_block, dict):
                    target_entry = agents_block.get(measurement.target_agent)
                    if isinstance(target_entry, dict):
                        bp_block = target_entry.get("backpack_accounts")
                        if isinstance(bp_block, dict):
                            dest_address = str(bp_block.get("internal_transfer_address") or "").strip() or None

            if not dest_address:
                raise ValueError(
                    f"Backpack auto balance: missing internal_transfer_address for target agent {measurement.target_agent}"
                )

            # Create a Backpack internal transfer request. The monitor will resolve
            # the destination address via payload.address or BACKPACK_INTERNAL_TRANSFER_PEER_ADDRESS.
            adj = self._backpack_adjustments.create(
                agent_id=measurement.source_agent,
                action="transfer_internal",
                magnitude=float(requested_amount),
                symbols=[cfg.currency],
                payload={
                    # These keys are interpreted by backpack_account_monitor.transfer_internal().
                    "address": dest_address,
                    "blockchain": "Internal",
                    "autoBorrow": False,
                    "autoLendRedeem": True,
                    "auto_balance": {
                        "venue": "backpack",
                        "ratio": measurement.ratio,
                        "threshold": cfg.threshold_ratio,
                        "difference": self._decimal_to_str(measurement.difference),
                        "agent_a_equity": self._decimal_to_str(measurement.equity_a),
                        "agent_b_equity": self._decimal_to_str(measurement.equity_b),
                        "computed_amount": self._decimal_to_str(measurement.transfer_amount),
                        "requested_amount": self._decimal_to_str(requested_amount),
                        "currency": cfg.currency,
                        "source_agent": measurement.source_agent,
                        "target_agent": measurement.target_agent,
                        "snapshot_ts": now,
                    },
                },
            )

            # Persist to local history for visibility (align with dashboard-created adjustments).
            entry: Dict[str, Any] = {
                "request_id": adj.request_id,
                "created_at": getattr(adj, "created_at", None),
                "action": "transfer_internal",
                "magnitude": float(requested_amount),
                "target_symbols": [cfg.currency],
                "symbols": [cfg.currency],
                "agent_id": measurement.source_agent,
                "overall_status": "pending",
                "status": "pending",
                "agents": [],
                "payload": {
                    "auto_balance": {
                        "source_agent": measurement.source_agent,
                        "target_agent": measurement.target_agent,
                        "currency": cfg.currency,
                        "requested_amount": self._decimal_to_str(requested_amount),
                        "snapshot_ts": now,
                    }
                },
            }
            self._bp_adjustments_history.insert(0, entry)
            if len(self._bp_adjustments_history) > self._bp_adjustments_history_limit:
                self._bp_adjustments_history = self._bp_adjustments_history[: self._bp_adjustments_history_limit]
            _atomic_write_json(BP_ADJUSTMENTS_HISTORY_FILE, self._bp_adjustments_history)

            self._bp_auto_balance_status["last_error"] = None
            self._bp_auto_balance_status["last_request_id"] = adj.request_id
            self._bp_auto_balance_status["last_action_at"] = now
            self._bp_auto_balance_status["last_transfer_amount"] = self._decimal_to_str(requested_amount)
            self._bp_auto_balance_status["last_direction"] = {
                "from": measurement.source_agent,
                "to": measurement.target_agent,
            }
        except Exception as exc:
            error_text = getattr(exc, "text", None) or str(exc)
            self._bp_auto_balance_status["last_error"] = error_text
            LOGGER.warning("Backpack auto balance transfer failed: %s", error_text)
        finally:
            if cfg.cooldown_seconds > 0:
                self._bp_auto_balance_cooldown_until = now + cfg.cooldown_seconds
            else:
                self._bp_auto_balance_cooldown_until = None
            self._bp_auto_balance_status["cooldown_until"] = self._bp_auto_balance_cooldown_until
            self._bp_auto_balance_status["cooldown_active"] = (
                self._bp_auto_balance_cooldown_until is not None and now < self._bp_auto_balance_cooldown_until
            )
            self._bp_auto_balance_status["pending"] = False
            self._bp_auto_balance_task = None

    async def _maybe_bp_auto_balance(self, snapshot: Dict[str, Any]) -> None:
        if not self._bp_auto_balance_cfg:
            return
        # Observe existing background task and clear if finished.
        if self._bp_auto_balance_task is not None:
            if self._bp_auto_balance_task.done():
                with suppress(Exception):
                    self._bp_auto_balance_task.result()
                self._bp_auto_balance_task = None
            else:
                self._bp_auto_balance_status["pending"] = True
                return
        async with self._bp_auto_balance_lock:
            cfg = self._bp_auto_balance_cfg
            measurement = self._compute_bp_auto_balance_measurement(snapshot)
            self._bp_auto_balance_status["measurement"] = measurement.as_payload() if measurement else None
            now = time.time()
            cooldown_until = self._bp_auto_balance_cooldown_until
            if cooldown_until is not None and now < cooldown_until:
                self._bp_auto_balance_status["cooldown_until"] = cooldown_until
                self._bp_auto_balance_status["cooldown_active"] = True
                return
            self._bp_auto_balance_status["cooldown_active"] = False
            self._bp_auto_balance_status["cooldown_until"] = cooldown_until
            if not measurement:
                return
            if measurement.ratio < cfg.threshold_ratio:
                return
            # Internal transfer amount: keep 2 decimal places (USDC cents) for predictability.
            # We round down to avoid requesting more than computed/available.
            requested_amount = measurement.transfer_amount.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if requested_amount < cfg.min_transfer:
                return
            if cfg.max_transfer is not None and requested_amount > cfg.max_transfer:
                requested_amount = cfg.max_transfer.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if requested_amount < cfg.min_transfer:
                return
            if requested_amount <= 0:
                return

            self._bp_auto_balance_status["pending"] = True
            self._bp_auto_balance_task = asyncio.create_task(
                self._run_bp_auto_balance_transfer(
                    measurement=measurement,
                    cfg=cfg,
                    requested_amount=requested_amount,
                )
            )
            return

    async def handle_backpack_adjustments_list(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        limit_raw = request.query.get("limit")
        limit = 10
        if limit_raw is not None and str(limit_raw).strip():
            try:
                limit = int(str(limit_raw).strip())
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text="limit must be an integer")
        limit = max(1, min(200, limit))
        payload = list(self._bp_adjustments_history[:limit])
        return web.json_response({"adjustments": payload, "limit": limit, "source": "persisted"})

    async def handle_backpack_adjustments_clear(self, request: web.Request) -> web.Response:
        """Clear persisted Backpack adjustment history.

        This deletes all locally persisted backpack adjust/transfer history entries
        (backed by `.bp_adjustments_history.json`). Used by the dashboard "clear history"
        button so operators can reset the panel.
        """
        self._enforce_dashboard_auth(request)
        try:
            self._bp_adjustments_history = []
            _atomic_write_json(BP_ADJUSTMENTS_HISTORY_FILE, self._bp_adjustments_history)
        except Exception as exc:
            raise web.HTTPInternalServerError(text=f"failed to clear history: {exc}")
        return web.json_response({"ok": True})

    async def handle_backpack_adjust_create(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="adjust payload must be JSON")
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="adjust payload must be an object")

        # Dashboard may broadcast to all backpack monitors, but for safety we should NOT
        # default to "all" when agent_id is omitted. The dashboard UI (PARA-style)
        # typically operates on a selected agent context; auto-broadcast leads to
        # duplicate requests and repeated TWAP creation across VPS.
        agent_id = str(body.get("agent_id") or "").strip() or None
        action = str(body.get("action") or "").strip().lower()
        if action not in {"add", "reduce", "transfer_internal"}:
            raise web.HTTPBadRequest(text="action must be 'add', 'reduce', or 'transfer_internal'")
        magnitude = body.get("magnitude")
        symbols_raw = body.get("symbols")

        # symbols:
        # - None / [] => broadcast all symbols (monitor-side should decide per-account held symbols)
        # - ["BTC-PERP", ...] => broadcast selected symbols
        if symbols_raw is None:
            symbols: List[str] = []
        elif isinstance(symbols_raw, str):
            symbols = [symbols_raw] if symbols_raw.strip() else []
        elif isinstance(symbols_raw, list):
            symbols = [str(s).strip() for s in symbols_raw if str(s).strip()]
        else:
            raise web.HTTPBadRequest(text="symbols must be a string or an array of strings")

        payload_cfg = body.get("payload")
        payload_dict = payload_cfg if isinstance(payload_cfg, dict) else {}

        # Pass-through common order hints from dashboard.
        order_mode_raw = str(body.get("order_mode") or "").strip().lower()
        if order_mode_raw in {"twap", "market"}:
            payload_dict.setdefault("order_mode", order_mode_raw)
        twap_raw = body.get("twap_duration_seconds")
        if twap_raw is not None:
            try:
                payload_dict.setdefault("twap_duration_seconds", int(twap_raw))
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text="twap_duration_seconds must be an integer")

        # Backpack native TWAP supports custom step/interval. Allow dashboard to specify
        # it as either interval_ms or twap_step_seconds, and provide a reasonable
        # default when only twap_duration_seconds is given.
        step_raw = body.get("twap_step_seconds")
        interval_ms_raw = body.get("interval_ms")
        if interval_ms_raw is not None or step_raw is not None:
            try:
                if interval_ms_raw is not None:
                    interval_ms = int(interval_ms_raw)
                else:
                    step_s = str(step_raw).strip()
                    interval_ms = int(float(step_s) * 1000)
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text="interval_ms/twap_step_seconds must be a number")
            if interval_ms <= 0:
                raise web.HTTPBadRequest(text="interval_ms must be > 0")
            payload_dict.setdefault("interval_ms", interval_ms)

        # Also map twap_duration_seconds -> duration_ms for the monitor's Strategy API.
        # Keep it best-effort; monitor has its own defaults.
        if "twap_duration_seconds" in payload_dict and "duration_ms" not in payload_dict:
            with suppress(Exception):
                payload_dict["duration_ms"] = int(payload_dict["twap_duration_seconds"]) * 1000

        # If user only set duration, pick a sane default step (5s) if not provided.
        if "duration_ms" in payload_dict and "interval_ms" not in payload_dict:
            payload_dict.setdefault("interval_ms", 5_000)

        # If agent_id is not provided, attempt to infer the current agent from
        # query param (used by some dashboards) or from the latest coordinator snapshot.
        resolved_agent_id = agent_id
        if not resolved_agent_id:
            with suppress(Exception):
                qp_agent = (request.rel_url.query.get("agent_id") or "").strip()
                if qp_agent:
                    resolved_agent_id = qp_agent
        if not resolved_agent_id:
            with suppress(Exception):
                snap = await self._coordinator.snapshot()
                agents_block = snap.get("agents")
                if isinstance(agents_block, dict):
                    # Prefer a single Backpack agent if we can unambiguously pick one.
                    candidate_ids = []
                    for k, v in agents_block.items():
                        if not isinstance(k, str):
                            continue
                        if not isinstance(v, dict):
                            continue
                        if "backpack_accounts" in v:
                            candidate_ids.append(k)
                    if len(candidate_ids) == 1:
                        resolved_agent_id = candidate_ids[0]

        if not resolved_agent_id:
            raise web.HTTPBadRequest(text="agent_id is required (refuse to default to broadcast 'all')")

        adj = self._backpack_adjustments.create(
            agent_id=resolved_agent_id,
            action=action,
            magnitude=magnitude,
            symbols=symbols,
            payload=payload_dict,
        )

        # Persist to local history immediately (status will be updated on ack).
        entry: Dict[str, Any] = {
            "request_id": adj.request_id,
            "created_at": getattr(adj, "created_at", None),
            "action": action,
            "magnitude": magnitude,
            "target_symbols": symbols,
            "symbols": symbols,
            "agent_id": resolved_agent_id,
            "overall_status": "pending",
            "status": "pending",
            "agents": [],
            "payload": payload_dict,
        }
        self._bp_adjustments_history.insert(0, entry)
        if len(self._bp_adjustments_history) > self._bp_adjustments_history_limit:
            self._bp_adjustments_history = self._bp_adjustments_history[: self._bp_adjustments_history_limit]
        _atomic_write_json(BP_ADJUSTMENTS_HISTORY_FILE, self._bp_adjustments_history)

        # Align response shape with PARA/GRVT adjustments.
        return web.json_response({"request": {"request_id": adj.request_id, "status": "queued"}})

    async def handle_backpack_adjust_ack(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="ack payload must be JSON")
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="ack payload must be an object")

        request_id = str(body.get("request_id") or "").strip()
        agent_id = str(body.get("agent_id") or "").strip()
        status = str(body.get("status") or "").strip().lower()
        note_raw = body.get("note")
        note = str(note_raw) if note_raw is not None else None
        if not request_id or not agent_id or not status:
            raise web.HTTPBadRequest(text="request_id, agent_id, status are required")

        extra = dict(body)
        for k in ("request_id", "agent_id", "status", "note"):
            extra.pop(k, None)

        ok = await self._backpack_adjustments.acknowledge(
            request_id=request_id,
            agent_id=agent_id,
            status=status,
            note=note,
            extra=extra,
        )
        if not ok:
            raise web.HTTPNotFound(text="unknown request_id")

        # Update persisted history entry.
        try:
            for item in self._bp_adjustments_history:
                if str(item.get("request_id") or "") != request_id:
                    continue
                agents = item.get("agents")
                if not isinstance(agents, list):
                    agents = []
                # upsert agent ack
                found: Optional[Dict[str, Any]] = None
                for row in agents:
                    if isinstance(row, dict) and str(row.get("agent_id") or "") == agent_id:
                        found = row  # type: ignore[assignment]
                        break
                if found is None:
                    found = {"agent_id": agent_id}
                    agents.append(found)
                found["status"] = status
                if note is not None:
                    found["note"] = note
                if extra:
                    found["extra"] = {str(k): v for k, v in extra.items()}
                item["agents"] = agents

                # recompute overall status (best-effort)
                statuses = [str(a.get("status") or "").lower() for a in agents if isinstance(a, dict)]
                success_set = {"completed", "succeeded", "success", "ok"}
                pending_set = {"pending", "in_progress", "queued"}
                failed_set = {"failed", "error"}

                if statuses and all(s in success_set for s in statuses):
                    overall = "completed"
                elif any(s in failed_set for s in statuses):
                    overall = "failed"
                elif any(s in pending_set for s in statuses):
                    overall = "in_progress"
                else:
                    overall = item.get("overall_status") or item.get("status") or "in_progress"
                item["overall_status"] = overall
                item["status"] = overall
                break
            _atomic_write_json(BP_ADJUSTMENTS_HISTORY_FILE, self._bp_adjustments_history)
        except Exception:
            # Keep ack path resilient; history persistence is best-effort.
            pass
        return web.json_response({"ok": True})

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

    async def handle_control_get(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        agent_id = (request.rel_url.query.get("agent_id") or "").strip()
        if not agent_id:
            raise web.HTTPBadRequest(text="agent_id is required")
        # Coordinator provides aggregated snapshot; extract per-agent control queue.
        snapshot = await self._coordinator.snapshot()
        controls = snapshot.get("controls")
        out: Dict[str, Any]
        if isinstance(controls, dict):
            per_agent = controls.get(agent_id)
            if isinstance(per_agent, dict):
                out = dict(per_agent)
            else:
                out = {"agent_id": agent_id, "pending_adjustments": []}
        else:
            out = {"agent_id": agent_id, "pending_adjustments": []}

        # IMPORTANT: also attach Backpack pending adjustments.
        # Backpack monitor executors poll /control; without this they will never
        # receive dashboard-created backpack adjustments, leaving them stuck in
        # pending state with 0 ACKs.
        with suppress(Exception):
            out["backpack_adjustments"] = await self._backpack_adjustments.pending_for_agent(agent_id)
        return web.json_response(out)

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

    async def handle_para_auto_balance_get(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload = {
            "config": self._para_auto_balance_config_as_payload(),
            "status": self._para_auto_balance_status_snapshot(),
        }
        return web.json_response(payload)

    async def handle_para_auto_balance_update(self, request: web.Request) -> web.Response:
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
            self._update_para_auto_balance_config(None)
            self._persist_para_auto_balance_config()
            return web.json_response({
                "config": None,
                "status": self._para_auto_balance_status_snapshot(),
            })

        try:
            config = self._parse_auto_balance_config(body, default_currency="USDC")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))

        self._update_para_auto_balance_config(config)
        self._persist_para_auto_balance_config()
        return web.json_response({
            "config": self._para_auto_balance_config_as_payload(),
            "status": self._para_auto_balance_status_snapshot(),
        })

    async def handle_risk_alert_settings(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload = await self._coordinator.alert_settings_snapshot()
        return web.json_response({"settings": payload})

    async def handle_para_risk_alert_settings(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload = await self._coordinator.para_alert_settings_snapshot()
        return web.json_response({"settings": payload})

    async def handle_bp_risk_alert_settings(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload = await self._coordinator.bp_alert_settings_snapshot()
        return web.json_response({"settings": payload})

    async def handle_risk_alert_history(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        limit_param = request.rel_url.query.get("limit")
        limit: Optional[int] = None
        if limit_param is not None:
            try:
                limit = max(1, min(int(limit_param), ALERT_HISTORY_LIMIT))
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text="limit must be a positive integer")
        history = await self._coordinator.alert_history_snapshot(limit=limit)
        return web.json_response({
            "history": history,
            "server": {"instance_id": SERVER_INSTANCE_ID, "pid": SERVER_PID},
        })

    async def handle_risk_alert_update(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="risk settings payload must be JSON")
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="risk settings payload must be an object")
        try:
            settings = await self._build_risk_alert_settings(body)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))
        payload = await self._coordinator.apply_alert_settings(settings)
        LOGGER.info("Risk alert settings updated via dashboard")
        return web.json_response({"settings": payload})

    async def handle_para_risk_alert_update(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="risk settings payload must be JSON")
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="risk settings payload must be an object")
        try:
            settings = await self._build_risk_alert_settings(body)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))
        payload = await self._coordinator.apply_para_alert_settings(settings)
        LOGGER.info("PARA risk alert settings updated via dashboard")
        return web.json_response({"settings": payload})

    async def handle_bp_risk_alert_update(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="risk settings payload must be JSON")
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="risk settings payload must be an object")
        try:
            settings = await self._build_risk_alert_settings(body)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))
        payload = await self._coordinator.apply_bp_alert_settings(settings)
        LOGGER.info("Backpack risk alert settings updated via dashboard")
        return web.json_response({"settings": payload})

    async def handle_risk_alert_test(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        body: Dict[str, Any] = {}
        raw_body: Optional[str] = None
        if request.can_read_body:
            try:
                raw_body = await request.text()
            except Exception:
                raw_body = None
            if raw_body and raw_body.strip():
                try:
                    parsed = json.loads(raw_body)
                except Exception:
                    raise web.HTTPBadRequest(text="test payload must be JSON")
                if not isinstance(parsed, dict):
                    raise web.HTTPBadRequest(text="test payload must be an object")
                body = parsed
        ratio = self._extract_numeric_field(body, ["ratio"], field_name="ratio") if body else None
        ratio_percent = self._extract_numeric_field(
            body,
            ["ratio_percent", "ratio_percentage"],
            field_name="ratio_percent",
            scale=0.01,
        ) if body else None
        overrides: Dict[str, Any] = {}
        if ratio is not None:
            overrides["ratio"] = ratio
        elif ratio_percent is not None:
            overrides["ratio"] = ratio_percent
        base_candidate = body.get("base_value") if body else None
        if base_candidate is None and body:
            base_candidate = body.get("base") or body.get("transferable")
        if base_candidate is not None:
            overrides["base_value"] = base_candidate
        loss_candidate = body.get("loss_value") if body else None
        if loss_candidate is None and body:
            loss_candidate = body.get("loss")
        if loss_candidate is not None:
            overrides["loss_value"] = loss_candidate
        if body:
            agent_id = self._clean_optional_string(body.get("agent_id"))
            account_label = self._clean_optional_string(body.get("account_label"))
            if agent_id:
                overrides["agent_id"] = agent_id
            if account_label:
                overrides["account_label"] = account_label

        kind_raw = None
        if isinstance(body.get("kind"), str):
            kind_raw = str(body.get("kind") or "").strip().lower() or None
        use_para = kind_raw in {"para", "para_risk", "para-risk"}
        if kind_raw is not None:
            overrides["kind"] = kind_raw
        try:
            payload = await self._coordinator.trigger_test_alert(overrides)
        except RuntimeError as exc:
            raise web.HTTPBadRequest(text=str(exc))
        LOGGER.info("Risk alert test triggered for Bark destination")
        return web.json_response({
            "alert": payload,
            "server": {"instance_id": SERVER_INSTANCE_ID, "pid": SERVER_PID},
            "debug": {
                "raw_body": raw_body,
                "parsed_kind": kind_raw,
                "use_para": use_para,
                "base_label": payload.get("base_label"),
            },
        })

    async def handle_grvt_adjustments(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        summary = await self._adjustments.summary()
        return web.json_response(summary)

    async def handle_para_adjustments(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        summary = await self._para_adjustments.summary()
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

    async def handle_para_adjust(self, request: web.Request) -> web.Response:
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
        # Default PARA adjustments to TWAP unless explicitly overridden.
        # (Dashboard / API can still force market by setting order_mode='market')
        order_mode_raw = str(body.get("order_mode") or "twap").strip().lower()
        extras: Dict[str, Any] = {}
        if order_mode_raw == "twap":
            duration_raw = body.get("twap_duration_seconds")
            try:
                duration_val = int(float(0 if duration_raw is None else duration_raw))
            except (TypeError, ValueError):
                duration_val = 900
            duration_val = max(30, min(86400, int(round(duration_val / 30) * 30)))
            extras["order_mode"] = "twap"
            extras["twap_duration_seconds"] = duration_val
            extras["algo_type"] = "TWAP"
        else:
            extras["order_mode"] = "market"
        try:
            payload = await self._para_adjustments.create_request(
                action=action,
                magnitude=magnitude,
                agent_ids=agent_ids,
                symbols=symbols,
                created_by=created_by,
                payload=extras,
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

        payload = await self._process_transfer_request(
            body,
            created_by=request.remote or "dashboard",
            defaults_provider=self._coordinator.get_agent_transfer_defaults,
            adjustments_manager=self._adjustments,
        )
        return web.json_response({"request": payload})

    async def handle_para_transfer(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="transfer payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="transfer payload must be an object")

        payload = await self._process_para_transfer_request(
            body,
            created_by=request.remote or "dashboard",
            adjustments_manager=self._para_adjustments,
            defaults_provider=self._coordinator.get_para_transfer_defaults,
        )
        return web.json_response({"request": payload})

    async def _process_para_transfer_request(
        self,
        body: Dict[str, Any],
        *,
        created_by: str,
        adjustments_manager: Optional[GrvtAdjustmentManager] = None,
        defaults_provider: Optional[Callable[[str], Awaitable[Optional[Dict[str, Any]]]]] = None,
    ) -> Dict[str, Any]:
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

        currency = _clean_required(body.get("currency") or "USDC", "currency").upper()

        agent_ids_raw = body.get("agent_ids")
        if agent_ids_raw is None:
            agent_ids = await self._coordinator.list_agent_ids()
        elif isinstance(agent_ids_raw, list):
            agent_ids = [str(agent).strip() for agent in agent_ids_raw if str(agent).strip()]
        else:
            raise web.HTTPBadRequest(text="agent_ids must be an array of strings")
        if not agent_ids:
            raise web.HTTPBadRequest(text="No agent IDs available for transfer request")
        if len(agent_ids) > 1:
            raise web.HTTPBadRequest(text="Paradex transfers support a single source agent per request")

        source_agent_id = agent_ids[0]
        target_agent_id = _clean_optional(
            body.get("target_agent_id")
            or body.get("destination_agent_id")
            or body.get("to_agent_id")
            or body.get("target_agent")
        )
        defaults_lookup = defaults_provider or self._coordinator.get_para_transfer_defaults
        source_defaults = await defaults_lookup(source_agent_id) if defaults_lookup else None
        target_defaults: Optional[Dict[str, Any]] = None
        if target_agent_id and defaults_lookup:
            target_defaults = await defaults_lookup(target_agent_id)

        target_l2_address = _clean_optional(
            body.get("target_l2_address")
            or body.get("recipient")
            or body.get("recipient_address")
            or body.get("to_address")
        )
        if not target_l2_address and target_defaults:
            target_l2_address = _clean_optional(target_defaults.get("l2_address"))
        if not target_l2_address:
            raise web.HTTPBadRequest(text="target_l2_address is required")

        metadata = body.get("transfer_metadata") or {}
        if metadata and not isinstance(metadata, dict):
            raise web.HTTPBadRequest(text="transfer_metadata must be an object if provided")
        metadata = dict(metadata or {})
        if body.get("reason") and not metadata.get("reason"):
            metadata["reason"] = str(body["reason"])
        metadata.setdefault("agent_id", source_agent_id)
        if source_defaults and source_defaults.get("agent_label"):
            metadata.setdefault("agent_label", str(source_defaults["agent_label"]))
        if target_agent_id:
            metadata.setdefault("target_agent_id", target_agent_id)
            if target_defaults and target_defaults.get("agent_label"):
                metadata.setdefault("target_agent_label", str(target_defaults["agent_label"]))
        metadata.setdefault("direction", "l2_transfer")

        transfer_payload: Dict[str, Any] = {
            "currency": currency,
            "num_tokens": format(amount, "f"),
            "target_l2_address": target_l2_address,
        }
        if metadata:
            transfer_payload["transfer_metadata"] = metadata

        manager = adjustments_manager or self._para_adjustments
        payload = await manager.create_request(
            action="transfer",
            magnitude=float(amount),
            agent_ids=agent_ids,
            symbols=None,
            created_by=created_by,
            payload=transfer_payload,
        )
        return payload

    async def _process_transfer_request(
        self,
        body: Dict[str, Any],
        *,
        created_by: str,
        defaults_provider: Optional[Callable[[str], Awaitable[Optional[Dict[str, Any]]]]] = None,
        adjustments_manager: Optional[GrvtAdjustmentManager] = None,
    ) -> Dict[str, Any]:
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

        defaults_lookup = defaults_provider or self._coordinator.get_agent_transfer_defaults
        transfer_defaults = await defaults_lookup(source_agent_id)
        target_defaults: Optional[Dict[str, Any]] = None
        if target_agent_id:
            # 对于 main_to_main 互转，允许目标端没有默认配置，只要请求体里提供了完整路由即可。
            # 如有配置则填充 metadata，缺失时继续执行，由后续字段校验兜底。
            target_defaults = await defaults_lookup(target_agent_id)

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

        manager = adjustments_manager or self._adjustments
        payload = await manager.create_request(
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
            source_payload = None
            agents_block = snapshot.get("agents")
            if isinstance(agents_block, dict):
                source_payload = agents_block.get(measurement.source_agent)
            source_transferable = self._extract_para_transferable_from_agent(source_payload) if source_payload else None
            if source_transferable is not None:
                self._para_auto_balance_status["source_transferable"] = self._decimal_to_str(source_transferable)
                if source_transferable <= 0:
                    self._para_auto_balance_status["last_error"] = "PARA auto balance skipped: no transferable funds at source"
                    return
                if requested_amount > source_transferable:
                    requested_amount = source_transferable
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

    def _para_auto_balance_status_snapshot(self) -> Dict[str, Any]:
        snapshot = copy.deepcopy(self._para_auto_balance_status)
        snapshot["enabled"] = bool(self._para_auto_balance_cfg)
        snapshot["config"] = self._para_auto_balance_config_as_payload()
        snapshot["pending"] = bool(self._para_auto_balance_task)
        return snapshot

    async def _maybe_para_auto_balance(self, snapshot: Dict[str, Any]) -> None:
        if not self._para_auto_balance_cfg:
            return
        # Observe existing background task and clear if finished
        if self._para_auto_balance_task is not None:
            if self._para_auto_balance_task.done():
                try:
                    self._para_auto_balance_task.result()
                except Exception as exc:  # pragma: no cover - log and continue
                    LOGGER.warning("PARA auto balance task failed: %s", exc)
                    self._para_auto_balance_status["last_error"] = str(exc)
                self._para_auto_balance_task = None
            else:
                self._para_auto_balance_status["pending"] = True
                return
        async with self._para_auto_balance_lock:
            cfg = self._para_auto_balance_cfg
            measurement = self._compute_para_auto_balance_measurement(snapshot)
            self._para_auto_balance_status["measurement"] = measurement.as_payload() if measurement else None
            now = time.time()
            cooldown_until = self._para_auto_balance_cooldown_until
            if cooldown_until is not None and now < cooldown_until:
                self._para_auto_balance_status["cooldown_until"] = cooldown_until
                self._para_auto_balance_status["cooldown_active"] = True
                return
            self._para_auto_balance_status["cooldown_active"] = False
            self._para_auto_balance_status["cooldown_until"] = cooldown_until
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
                    "venue": "paradex",
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
            reason = f"para_auto_balance {measurement.source_agent}->{measurement.target_agent}"
            request_body = {
                "agent_ids": [measurement.source_agent],
                "target_agent_id": measurement.target_agent,
                "currency": cfg.currency,
                "num_tokens": self._decimal_to_str(requested_amount),
                "transfer_metadata": metadata,
                "reason": reason,
            }
            # Dispatch transfer asynchronously to avoid blocking monitor updates
            self._para_auto_balance_status["pending"] = True
            self._para_auto_balance_task = asyncio.create_task(
                self._run_para_auto_balance_transfer(
                    request_body=request_body,
                    measurement=measurement,
                    cfg=cfg,
                    requested_amount=requested_amount,
                )
            )
            return

    def _compute_para_auto_balance_measurement(self, snapshot: Dict[str, Any]) -> Optional[AutoBalanceMeasurement]:
        if not self._para_auto_balance_cfg:
            return None
        agents_block = snapshot.get("agents")
        if not isinstance(agents_block, dict):
            return None
        entry_a = agents_block.get(self._para_auto_balance_cfg.agent_a)
        entry_b = agents_block.get(self._para_auto_balance_cfg.agent_b)
        if not isinstance(entry_a, dict) or not isinstance(entry_b, dict):
            return None
        equity_a = self._extract_para_equity_from_agent(entry_a, prefer_available=self._para_auto_balance_cfg.use_available_equity)
        equity_b = self._extract_para_equity_from_agent(entry_b, prefer_available=self._para_auto_balance_cfg.use_available_equity)
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
        source_agent = self._para_auto_balance_cfg.agent_a if equity_a >= equity_b else self._para_auto_balance_cfg.agent_b
        target_agent = self._para_auto_balance_cfg.agent_b if source_agent == self._para_auto_balance_cfg.agent_a else self._para_auto_balance_cfg.agent_a
        transfer_amount = difference / Decimal("2")
        if transfer_amount <= 0:
            return None
        return AutoBalanceMeasurement(
            agent_a=self._para_auto_balance_cfg.agent_a,
            agent_b=self._para_auto_balance_cfg.agent_b,
            equity_a=equity_a,
            equity_b=equity_b,
            difference=difference,
            ratio=ratio,
            source_agent=source_agent,
            target_agent=target_agent,
            transfer_amount=transfer_amount,
        )

    async def _run_para_auto_balance_transfer(
        self,
        *,
        request_body: Dict[str, Any],
        measurement: AutoBalanceMeasurement,
        cfg: AutoBalanceConfig,
        requested_amount: Decimal,
    ) -> None:
        now = time.time()
        try:
            response = await self._process_para_transfer_request(
                request_body,
                created_by="para_auto_balance",
                adjustments_manager=self._para_adjustments,
                defaults_provider=self._coordinator.get_para_transfer_defaults,
            )
        except Exception as exc:
            error_text = getattr(exc, "text", None) or str(exc)
            self._para_auto_balance_status["last_error"] = error_text
            if cfg.cooldown_seconds > 0:
                self._para_auto_balance_cooldown_until = now + cfg.cooldown_seconds
            else:
                self._para_auto_balance_cooldown_until = None
            self._para_auto_balance_status["cooldown_until"] = self._para_auto_balance_cooldown_until
            self._para_auto_balance_status["cooldown_active"] = (
                self._para_auto_balance_cooldown_until is not None and now < self._para_auto_balance_cooldown_until
            )
            LOGGER.warning("PARA auto balance transfer failed: %s", error_text)
            self._para_auto_balance_status["pending"] = False
            self._para_auto_balance_task = None
            return

        self._para_auto_balance_status["last_error"] = None
        self._para_auto_balance_status["last_request_id"] = response.get("request_id")
        self._para_auto_balance_status["last_action_at"] = now
        self._para_auto_balance_status["last_transfer_amount"] = self._decimal_to_str(requested_amount)
        self._para_auto_balance_status["last_direction"] = {
            "from": measurement.source_agent,
            "to": measurement.target_agent,
        }
        if cfg.cooldown_seconds > 0:
            self._para_auto_balance_cooldown_until = now + cfg.cooldown_seconds
        else:
            self._para_auto_balance_cooldown_until = None
        self._para_auto_balance_status["cooldown_until"] = self._para_auto_balance_cooldown_until
        self._para_auto_balance_status["cooldown_active"] = (
            self._para_auto_balance_cooldown_until is not None and now < self._para_auto_balance_cooldown_until
        )
        self._para_auto_balance_status["pending"] = False
        self._para_auto_balance_task = None

    def _extract_para_equity_from_agent(self, agent_payload: Dict[str, Any], *, prefer_available: bool) -> Optional[Decimal]:
        para_block = agent_payload.get("paradex_accounts")
        if not isinstance(para_block, dict):
            return None
        summary = para_block.get("summary")
        summary_block = summary if isinstance(summary, dict) else None
        if summary_block is None:
            return None
        fields_available = ("available_equity", "available_balance", "equity", "balance")
        fields_equity_first = ("equity", "balance", "available_equity", "available_balance")
        fields = fields_available if prefer_available else fields_equity_first
        for field in fields:
            value = HedgeCoordinator._decimal_from(summary_block.get(field))
            if value is not None:
                return value
        return None

    def _extract_para_transferable_from_agent(self, agent_payload: Optional[Dict[str, Any]]) -> Optional[Decimal]:
        if not isinstance(agent_payload, dict):
            return None
        para_block = agent_payload.get("paradex_accounts")
        if not isinstance(para_block, dict):
            return None
        summary = para_block.get("summary")
        if not isinstance(summary, dict):
            return None
        candidates = (
            summary.get("available_equity"),
            summary.get("available_balance"),
            summary.get("equity"),
            summary.get("balance"),
        )
        values: List[Decimal] = []
        for raw in candidates:
            value = HedgeCoordinator._decimal_from(raw)
            if value is not None:
                values.append(value)
        if not values:
            return None
        positives = [v for v in values if v > 0]
        if not positives:
            return Decimal("0")
        return min(positives)

    def _extract_equity_from_agent(self, agent_payload: Dict[str, Any], *, prefer_available: bool) -> Optional[Decimal]:
        grvt_block = agent_payload.get("grvt_accounts")
        if not isinstance(grvt_block, dict):
            return None
        summary = grvt_block.get("summary")
        summary_block = summary if isinstance(summary, dict) else None
        if prefer_available:
            transferable = self._decimal_from_snapshot(agent_payload.get("grvt_transferable_balance"))
            if transferable is not None:
                return transferable
        if summary_block is None:
            return None
        if prefer_available:
            fields = ("available_equity", "available_balance", "equity", "balance")
        else:
            fields = ("equity", "balance", "available_equity", "available_balance")
        for field in fields:
            value = HedgeCoordinator._decimal_from(summary_block.get(field))
            if value is not None:
                return value
        return None

    @staticmethod
    def _decimal_from_snapshot(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    @staticmethod
    def _decimal_to_str(value: Decimal) -> str:
        try:
            return format(value, "f")
        except Exception:
            return str(value)

    def _auto_balance_config_as_payload(self, config: Optional[AutoBalanceConfig] = None) -> Optional[Dict[str, Any]]:
        cfg = config if config is not None else self._auto_balance_cfg
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

    def _para_auto_balance_config_as_payload(self) -> Optional[Dict[str, Any]]:
        return self._auto_balance_config_as_payload(self._para_auto_balance_cfg)

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

    def _update_para_auto_balance_config(self, config: Optional[AutoBalanceConfig]) -> None:
        self._para_auto_balance_cfg = config
        self._para_auto_balance_cooldown_until = None
        self._para_auto_balance_status["enabled"] = bool(config)
        self._para_auto_balance_status["config"] = self._para_auto_balance_config_as_payload()
        self._para_auto_balance_status["cooldown_until"] = None
        self._para_auto_balance_status["cooldown_active"] = False
        self._para_auto_balance_status["measurement"] = None
        self._para_auto_balance_status["last_error"] = None
        self._para_auto_balance_status["last_request_id"] = None
        self._para_auto_balance_status["last_action_at"] = None
        self._para_auto_balance_status["last_transfer_amount"] = None
        self._para_auto_balance_status["last_direction"] = None
        if not config:
            LOGGER.info("PARA auto balance disabled via dashboard")
        else:
            try:
                min_text = self._decimal_to_str(config.min_transfer)
            except Exception:
                min_text = str(config.min_transfer)
            LOGGER.info(
                "PARA auto balance configured via dashboard: %s ↔ %s (threshold %.2f%%, min %s %s)",
                config.agent_a,
                config.agent_b,
                config.threshold_ratio * 100,
                min_text,
                config.currency,
            )

    def _parse_auto_balance_config(self, body: Dict[str, Any], *, default_currency: str = "USDT") -> AutoBalanceConfig:
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

        currency_raw = body.get("currency") or default_currency or "USDT"
        currency = str(currency_raw).strip().upper() or str(default_currency or "USDT").strip().upper() or "USDT"
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

    @staticmethod
    def _clean_optional_string(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            text = str(value).strip()
        except Exception:
            return None
        return text or None

    def _extract_numeric_field(
        self,
        payload: Mapping[str, Any],
        keys: Sequence[str],
        *,
        field_name: str,
        scale: float = 1.0,
    ) -> Optional[float]:
        for key in keys:
            if key not in payload:
                continue
            value = payload[key]
            if value is None:
                return None
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
            try:
                number = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{field_name} must be numeric")
            return number * scale
        return None

    async def _build_risk_alert_settings(self, payload: Mapping[str, Any]) -> RiskAlertSettings:
        snapshot = await self._coordinator.alert_settings_snapshot()
        base = RiskAlertSettings()
        base.threshold = snapshot.get("threshold")
        base.reset_ratio = snapshot.get("reset_ratio")
        base.cooldown = snapshot.get("cooldown", base.cooldown)
        base.bark_url = snapshot.get("bark_url")
        base.bark_append_payload = snapshot.get("bark_append_payload", base.bark_append_payload)
        base.bark_timeout = snapshot.get("bark_timeout", base.bark_timeout)
        base.title_template = snapshot.get("title_template", base.title_template)
        base.body_template = snapshot.get("body_template", base.body_template)

        enabled_flag = snapshot.get("enabled", bool(base.threshold))
        if "enabled" in payload:
            enabled_flag = self._interpret_bool(payload.get("enabled"))

        threshold_value = self._extract_numeric_field(payload, ["threshold", "threshold_ratio"], field_name="threshold")
        threshold_percent = self._extract_numeric_field(
            payload,
            ["threshold_percent", "threshold_percentage"],
            field_name="threshold_percent",
            scale=0.01,
        )
        if threshold_percent is not None:
            threshold_value = threshold_percent
        if threshold_value is not None:
            base.threshold = threshold_value
            enabled_flag = True

        reset_value = self._extract_numeric_field(payload, ["reset_ratio", "reset"], field_name="reset_ratio")
        reset_percent = self._extract_numeric_field(
            payload,
            ["reset_ratio_percent", "reset_percent"],
            field_name="reset_ratio_percent",
            scale=0.01,
        )
        if reset_percent is not None:
            reset_value = reset_percent
        if reset_value is not None:
            base.reset_ratio = reset_value

        cooldown_value = self._extract_numeric_field(
            payload,
            ["cooldown", "cooldown_seconds"],
            field_name="cooldown",
        )
        cooldown_minutes = self._extract_numeric_field(
            payload,
            ["cooldown_minutes"],
            field_name="cooldown_minutes",
            scale=60.0,
        )
        if cooldown_minutes is not None:
            cooldown_value = cooldown_minutes
        if cooldown_value is not None:
            base.cooldown = max(cooldown_value, 0.0)

        bark_url = payload.get("bark_url")
        if bark_url is None and "url" in payload:
            bark_url = payload.get("url")
        url_value = self._clean_optional_string(bark_url)
        if url_value is not None or ("bark_url" in payload or "url" in payload):
            base.bark_url = url_value

        append_field = None
        if "bark_append_payload" in payload:
            append_field = payload.get("bark_append_payload")
        elif "bark_append_title_body" in payload:
            append_field = payload.get("bark_append_title_body")
        if append_field is not None:
            base.bark_append_payload = self._interpret_bool(append_field)

        bark_timeout = self._extract_numeric_field(payload, ["bark_timeout"], field_name="bark_timeout")
        if bark_timeout is not None:
            base.bark_timeout = max(bark_timeout, 1.0)

        title_value = self._clean_optional_string(payload.get("title_template") or payload.get("title"))
        if title_value is not None:
            base.title_template = title_value

        body_value = self._clean_optional_string(payload.get("body_template") or payload.get("body"))
        if body_value is not None:
            base.body_template = body_value

        if not enabled_flag:
            base.threshold = None
            base.reset_ratio = None

        return base.normalized()

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

    async def handle_para_adjust_ack(self, request: web.Request) -> web.Response:
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

        progress_flag = body.get("progress")
        progress = bool(progress_flag) if progress_flag is not None else False

        # Optional structured execution fields sent by PARA monitors.
        ack_extra: Dict[str, Any] = {}
        for key in (
            "order_type",
            "avg_price",
            "filled_qty",
            # Prefer explicit target size when monitors provide it.
            "total_qty",
            "target_qty",
            "order_id",
            # Algo order status fields (Paradex AlgoOrderResp)
            "algo_id",
            "algo_status",
            "algo_last_updated_at",
            "algo_remaining_size",
            "algo_size",
            "algo_expected_size",
            # Raw history timestamps for dashboard time/duration.
            "created_at",
            "last_updated_at",
            "updated_at",
            "end_at",
        ):
            if key in body:
                ack_extra[key] = body.get(key)

        if not request_id:
            raise web.HTTPBadRequest(text="request_id is required")
        if not agent_id:
            raise web.HTTPBadRequest(text="agent_id is required")

        try:
            payload = await self._para_adjustments.acknowledge(
                request_id=request_id,
                agent_id=agent_id,
                status=status,
                note=(note if isinstance(note, str) else None),
                extra=ack_extra,
                progress=progress,
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

    def _sanitize_binance_symbol(self, value: Any, default: str) -> str:
        default_clean = self._normalize_symbol(default) or "BTCUSDT"
        cleaned = self._normalize_symbol(value)
        if not cleaned or cleaned in {"NONE", "NULL", "DEFAULT", "N/A", "NA", "UNKNOWN"}:
            cleaned = default_clean
        suffixes = ("USDT", "USD", "USDC", "BUSD", "FDUSD", "TRY", "EUR")
        if any(cleaned.endswith(suffix) for suffix in suffixes):
            return cleaned
        if len(cleaned) <= 5:
            return f"{cleaned}USDT"
        return cleaned

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

    # Silence aiohttp access logs by default (they are very noisy in production).
    # Set AIOHTTP_ACCESS_LOG=1/true to re-enable access logs.
    access_log_enabled = _env_bool("AIOHTTP_ACCESS_LOG", False)
    if not access_log_enabled:
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    risk_threshold = args.risk_alert_threshold if args.risk_alert_threshold and args.risk_alert_threshold > 0 else None
    risk_reset = None
    if risk_threshold and args.risk_alert_reset and args.risk_alert_reset > 0:
        risk_reset = min(args.risk_alert_reset, risk_threshold)
    bark_url = (args.bark_url or "").strip()
    alert_settings = RiskAlertSettings(
        threshold=risk_threshold,
        reset_ratio=risk_reset,
        cooldown=max(args.risk_alert_cooldown, 0.0),
        bark_url=bark_url or None,
        bark_append_payload=bool(args.bark_append_title_body),
        bark_timeout=max(args.bark_timeout, 1.0),
        title_template=args.bark_title_template,
        body_template=args.bark_body_template,
    ).normalized()

    scope_raw = (getattr(args, "risk_alert_scope", "both") or "both").strip().lower()
    if scope_raw not in {"both", "global", "para", "bp", "none"}:
        raise SystemExit("--risk-alert-scope must be one of: both, global, para, bp, none")

    global_settings: Optional[RiskAlertSettings] = alert_settings if scope_raw in {"both", "global"} else None
    para_settings: Optional[RiskAlertSettings] = alert_settings if scope_raw in {"both", "para"} else None
    bp_settings: Optional[RiskAlertSettings] = alert_settings if scope_raw in {"both", "bp"} else None

    if alert_settings.threshold and alert_settings.bark_url:
        LOGGER.info(
            "Bark notifications enabled; risk alerts fire at %.1f%%",
            alert_settings.threshold * 100,
        )
        if not alert_settings.bark_append_payload:
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
        alert_settings=global_settings,
    )

    # Apply PARA settings after app starts so that the CLI can choose which scopes are enabled.
    if para_settings is not None:
        try:
            coordinator_app._coordinator._apply_para_alert_settings(para_settings)
        except Exception:
            LOGGER.exception("Failed to apply PARA alert settings")

    if bp_settings is not None:
        try:
            coordinator_app._coordinator._apply_bp_alert_settings(bp_settings)
        except Exception:
            LOGGER.exception("Failed to apply Backpack alert settings")

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
        try:
            loop.add_signal_handler(sig, _shutdown_handler)
        except (NotImplementedError, RuntimeError):
            # Windows (ProactorEventLoop) doesn't support add_signal_handler.
            # Fallback: rely on Ctrl+C / process termination; we also keep the server
            # alive instead of crashing at startup.
            LOGGER.warning("Signal handlers not supported on this event loop (%s); graceful shutdown via signals disabled", type(loop).__name__)

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
        default=_env_float("DASHBOARD_SESSION_TTL", 7 * 24 * 3600),
        help="Seconds that a dashboard login session remains valid (default 7 days).",
    )
    parser.add_argument(
        "--risk-alert-scope",
        default=os.getenv("RISK_ALERT_SCOPE", "both"),
        choices=["both", "global", "para", "bp", "none"],
        help="Which risk alert scope(s) to enable: both|global|para|bp|none (default both).",
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
    parser.add_argument(
        "--bark-title-template",
        default=os.getenv("BARK_TITLE_TEMPLATE", "GRVT Risk {ratio_percent:.1f}%"),
        help="Template for Bark alert titles.",
    )
    parser.add_argument(
        "--bark-body-template",
        default=
        os.getenv(
            "BARK_BODY_TEMPLATE",
            "{account_label} ({agent_id}) loss {loss_value} / {base_label} {base_value}",
        ),
        help="Template for Bark alert bodies.",
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
