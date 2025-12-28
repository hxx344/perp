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
import os
import secrets
import signal
import statistics
import time
from datetime import datetime, timezone
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple, cast
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
GLOBAL_RISK_ALERT_KEY = "__global_risk__"
PARA_RISK_ALERT_KEY = "__para_risk__"
TRANSFERABLE_HISTORY_LIMIT = 720
TRANSFERABLE_HISTORY_MERGE_SECONDS = 20.0
ALERT_HISTORY_LIMIT = 200

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


class VolatilityMonitor:
    """Background task that polls spot prices to derive BTC/ETH volatility signals."""

    _ENDPOINT = "https://api.binance.com/api/v3/klines"
    _DEFAULT_SYMBOLS: Tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    _DEFAULT_TIMEFRAMES: Tuple[Tuple[str, int], ...] = (
        ("15m", 15),
        ("1h", 60),
        ("4h", 240),
        ("24h", 1440),
    )
    _PRICE_SERIES_LIMIT = 720

    def __init__(
        self,
        *,
        symbols: Optional[Sequence[str]] = None,
        poll_interval: float = 60.0,
        history_limit: int = 1800,
        timeout: float = 10.0,
    ) -> None:
        cleaned_symbols: List[str] = []
        for symbol in symbols or self._DEFAULT_SYMBOLS:
            if not symbol:
                continue
            normalized = str(symbol).strip().upper()
            if normalized and normalized not in cleaned_symbols:
                cleaned_symbols.append(normalized)
        if not cleaned_symbols:
            cleaned_symbols = list(self._DEFAULT_SYMBOLS)

        self._symbols: Tuple[str, ...] = tuple(cleaned_symbols)
        self._symbol_labels: Dict[str, str] = {
            symbol: self._base_label(symbol) for symbol in self._symbols
        }
        self._pair: Optional[Tuple[str, str]] = (
            (self._symbols[0], self._symbols[1]) if len(self._symbols) >= 2 else None
        )
        self._timeframes: Tuple[Tuple[str, int], ...] = self._DEFAULT_TIMEFRAMES
        max_minutes = max(minutes for _, minutes in self._timeframes)
        history_floor = max_minutes + 5
        history_cap = max(history_floor, 2500)
        try:
            requested_history = int(history_limit)
        except (TypeError, ValueError):
            requested_history = history_floor
        if requested_history <= 0:
            requested_history = history_floor
        self._history_limit = max(history_floor, min(requested_history, history_cap))
        self._poll_interval = max(float(poll_interval), 20.0)
        self._timeout = max(float(timeout), 3.0)
        self._session: Optional[ClientSession] = None
        self._task: Optional[asyncio.Task[Any]] = None
        self._stopped = False
        self._histories: Dict[str, Deque[Dict[str, float]]] = {
            symbol: deque(maxlen=self._history_limit) for symbol in self._symbols
        }
        self._chart_history_points = min(self._history_limit, self._PRICE_SERIES_LIMIT)
        self._snapshot: Optional[Dict[str, Any]] = None
        self._lock = asyncio.Lock()
        self._last_error: Optional[str] = None

    async def start(self) -> None:
        if self._task or not self._symbols:
            return
        self._stopped = False
        self._session = ClientSession(timeout=ClientTimeout(total=self._timeout))
        try:
            await self._refresh_all()
        except Exception as exc:  # pragma: no cover - network bootstrap path
            LOGGER.warning("Initial volatility refresh failed: %s", exc)
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stopped = True
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def snapshot(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if self._snapshot is None:
                return None
            return copy.deepcopy(self._snapshot)

    async def _run_loop(self) -> None:
        try:
            while not self._stopped:
                await asyncio.sleep(self._poll_interval)
                await self._refresh_all()
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            raise
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("Volatility monitor task stopped: %s", exc)

    def _ensure_session(self) -> ClientSession:
        if self._session is None or self._session.closed:
            self._session = ClientSession(timeout=ClientTimeout(total=self._timeout))
        return self._session

    async def _refresh_all(self) -> None:
        if not self._symbols:
            return
        latest_error: Optional[str] = None
        any_success = False
        for symbol in self._symbols:
            try:
                await self._fetch_symbol(symbol)
                any_success = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - network path
                latest_error = f"{symbol}: {exc}"
                LOGGER.warning("Volatility fetch for %s failed: %s", symbol, exc)
        if latest_error:
            self._last_error = latest_error
        elif any_success:
            self._last_error = None
        await self._update_snapshot()

    async def _fetch_symbol(self, symbol: str) -> None:
        session = self._ensure_session()
        target = self._history_limit
        if target <= 0:
            target = 500
        collected: List[Dict[str, float]] = []
        end_time_ms: Optional[int] = None
        api_limit = 1000

        while len(collected) < target:
            batch_size = min(api_limit, target - len(collected))
            params: Dict[str, Any] = {
                "symbol": symbol,
                "interval": "1m",
                "limit": str(batch_size),
            }
            if end_time_ms is not None:
                params["endTime"] = end_time_ms
            async with session.get(self._ENDPOINT, params=params) as response:
                if response.status != 200:
                    body_preview = (await response.text())[:200]
                    raise RuntimeError(f"{symbol}: HTTP {response.status} {body_preview}")
                payload = await response.json()
            if not isinstance(payload, list):
                raise RuntimeError("Unexpected payload type from Binance")
            if not payload:
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
                        }
                    )
                except (IndexError, TypeError, ValueError):
                    continue
            if not chunk:
                break

            # prepend older chunk so the list stays chronological
            collected = chunk + collected

            first_open = payload[0][0]
            try:
                end_time_ms = int(first_open) - 1
            except (TypeError, ValueError):
                end_time_ms = None
            if len(payload) < batch_size:
                break

        if not collected:
            return
        if len(collected) > target:
            collected = collected[-target:]
        self._histories[symbol] = deque(collected, maxlen=self._history_limit)

    async def _update_snapshot(self) -> None:
        snapshot = self._build_snapshot()
        async with self._lock:
            self._snapshot = snapshot

    def _build_snapshot(self) -> Optional[Dict[str, Any]]:
        symbols_payload: Dict[str, Any] = {}
        price_series_payload: Dict[str, Any] = {}
        for symbol in self._symbols:
            payload = self._compute_symbol_metrics(symbol)
            if payload:
                symbols_payload[symbol] = payload
            series = self._serialize_price_series(symbol)
            if series:
                price_series_payload[symbol] = {
                    "symbol": symbol,
                    "label": self._symbol_labels.get(symbol, symbol),
                    "series": series,
                }
        if not symbols_payload:
            return None
        snapshot: Dict[str, Any] = {
            "updated_at": time.time(),
            "source": "binance",
            "symbols": symbols_payload,
            "timeframes": [label for label, _ in self._timeframes],
        }
        if price_series_payload:
            snapshot["price_series"] = price_series_payload
        guidance = self._compute_pair_guidance(symbols_payload)
        if guidance:
            snapshot["hedge_guidance"] = guidance
        if self._last_error:
            snapshot["last_error"] = self._last_error
        return snapshot

    def _compute_symbol_metrics(self, symbol: str) -> Optional[Dict[str, Any]]:
        history = self._histories.get(symbol)
        if not history or len(history) < 5:
            return None
        candles = list(history)
        last_candle = candles[-1]
        timeframes_payload: Dict[str, Any] = {}
        for label, minutes in self._timeframes:
            tf_payload = self._compute_timeframe_metrics(symbol, minutes)
            if tf_payload:
                timeframes_payload[label] = tf_payload
        if not timeframes_payload:
            return None
        return {
            "symbol": symbol,
            "label": self._symbol_labels.get(symbol, symbol),
            "last_price": last_candle.get("close"),
            "last_updated": last_candle.get("close_time"),
            "timeframes": timeframes_payload,
        }

    def _compute_timeframe_metrics(self, symbol: str, minutes: int) -> Optional[Dict[str, Any]]:
        if minutes <= 1:
            return None
        window = self._window_candles(symbol, minutes, include_anchor=True)
        if len(window) < minutes + 1:
            return None
        closes = [entry["close"] for entry in window]
        first_close = closes[0]
        last_close = closes[-1]
        if first_close <= 0 or last_close <= 0:
            return None
        returns = self._window_returns(symbol, minutes)
        realized_vol = self._annualized_vol(returns, minutes)
        return_pct = ((last_close / first_close) - 1.0) * 100.0
        atr_values = self._compute_atr(symbol, minutes)
        amplitude_pct = self._compute_amplitude_pct(symbol, minutes, last_close)
        payload: Dict[str, Any] = {
            "return_pct": return_pct,
            "sample_count": len(window) - 1,
        }
        if realized_vol is not None:
            payload["realized_vol_pct"] = realized_vol
        if atr_values:
            payload.update(atr_values)
        if amplitude_pct is not None:
            payload["amplitude_pct"] = amplitude_pct
        return payload

    def _serialize_price_series(self, symbol: str) -> Optional[List[Dict[str, float]]]:
        history = self._histories.get(symbol)
        if not history:
            return None
        window = list(history)[-self._chart_history_points :]
        if len(window) < 2:
            return None
        series: List[Dict[str, float]] = []
        for candle in window:
            ts = candle.get("close_time")
            price = candle.get("close")
            if isinstance(ts, (int, float)) and isinstance(price, (int, float)):
                series.append({"ts": ts, "price": price})
        if len(series) < 2:
            return None
        return series

    def _compute_atr(self, symbol: str, minutes: int) -> Optional[Dict[str, Any]]:
        window = self._window_candles(symbol, minutes)
        if len(window) < minutes:
            return None
        prev_close = self._window_candles(symbol, minutes, include_anchor=True)
        if len(prev_close) < minutes + 1:
            return None
        prev_value = prev_close[0]["close"]
        true_ranges: List[float] = []
        for candle in window:
            high = candle["high"]
            low = candle["low"]
            tr = max(
                high - low,
                abs(high - prev_value),
                abs(low - prev_value),
            )
            true_ranges.append(tr)
            prev_value = candle["close"]
        if not true_ranges:
            return None
        atr_abs = sum(true_ranges) / len(true_ranges)
        last_close = window[-1]["close"]
        atr_pct = (atr_abs / last_close * 100.0) if last_close > 0 else None
        payload = {"atr_abs": atr_abs}
        if atr_pct is not None:
            payload["atr_pct"] = atr_pct
        return payload

    def _compute_amplitude_pct(self, symbol: str, minutes: int, last_close: float) -> Optional[float]:
        window = self._window_candles(symbol, minutes)
        if len(window) < minutes or last_close <= 0:
            return None
        high = max(candle["high"] for candle in window)
        low = min(candle["low"] for candle in window)
        if not math.isfinite(high) or not math.isfinite(low) or high <= 0 or low <= 0:
            return None
        return ((high - low) / last_close) * 100.0

    def _window_candles(self, symbol: str, count: int, *, include_anchor: bool = False) -> List[Dict[str, float]]:
        history = self._histories.get(symbol)
        if not history:
            return []
        needed = count + 1 if include_anchor else count
        if len(history) < needed:
            return []
        return list(history)[-needed:]

    def _window_returns(self, symbol: str, minutes: int) -> List[float]:
        window = self._window_candles(symbol, minutes, include_anchor=True)
        returns: List[float] = []
        if len(window) < minutes + 1:
            return returns
        for index in range(1, len(window)):
            prev_close = window[index - 1]["close"]
            close = window[index]["close"]
            if prev_close > 0 and close > 0:
                returns.append(math.log(close / prev_close))
        return returns

    @staticmethod
    def _annualized_vol(returns: Sequence[float], window_minutes: int) -> Optional[float]:
        if len(returns) < 2 or window_minutes <= 0:
            return None
        variance = statistics.pvariance(returns)
        if variance <= 0:
            return 0.0
        minutes_per_year = 60 * 24 * 365
        scale = math.sqrt(minutes_per_year / float(window_minutes))
        return math.sqrt(variance) * scale * 100.0

    def _compute_pair_guidance(self, payloads: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._pair:
            return None
        base_symbol, quote_symbol = self._pair
        if base_symbol not in payloads or quote_symbol not in payloads:
            return None
        timeframe_stats: Dict[str, Any] = {}
        for label, minutes in self._timeframes:
            stats = self._pair_timeframe_stats(base_symbol, quote_symbol, minutes)
            if stats:
                timeframe_stats[label] = stats
        if not timeframe_stats:
            return None
        return {
            "pair": f"{self._symbol_labels.get(quote_symbol, quote_symbol)}/{self._symbol_labels.get(base_symbol, base_symbol)}",
            "base_symbol": base_symbol,
            "quote_symbol": quote_symbol,
            "timeframes": timeframe_stats,
        }

    def _pair_timeframe_stats(self, base_symbol: str, quote_symbol: str, minutes: int) -> Optional[Dict[str, Any]]:
        base_returns = self._window_returns(base_symbol, minutes)
        quote_returns = self._window_returns(quote_symbol, minutes)
        n = min(len(base_returns), len(quote_returns))
        if n < 2:
            return None
        base_series = base_returns[-n:]
        quote_series = quote_returns[-n:]
        mean_base = statistics.fmean(base_series)
        mean_quote = statistics.fmean(quote_series)
        cov = sum((a - mean_base) * (b - mean_quote) for a, b in zip(base_series, quote_series)) / n
        var_base = statistics.pvariance(base_series)
        var_quote = statistics.pvariance(quote_series)
        if var_base <= 0 or var_quote <= 0:
            return None
        corr = cov / math.sqrt(var_base * var_quote)
        vol_ratio = math.sqrt(var_quote / var_base)
        beta = corr * vol_ratio
        base_return_pct = self._compute_return_pct(base_symbol, minutes)
        quote_return_pct = self._compute_return_pct(quote_symbol, minutes)
        spread = None
        if base_return_pct is not None and quote_return_pct is not None:
            spread = quote_return_pct - base_return_pct
        payload: Dict[str, Any] = {
            "corr": corr,
            "vol_ratio": vol_ratio,
            "beta": beta,
        }
        if spread is not None:
            payload["return_spread_pct"] = spread
        return payload

    def _compute_return_pct(self, symbol: str, minutes: int) -> Optional[float]:
        window = self._window_candles(symbol, minutes, include_anchor=True)
        if len(window) < minutes + 1:
            return None
        first = window[0]["close"]
        last = window[-1]["close"]
        if first <= 0 or last <= 0:
            return None
        return ((last / first) - 1.0) * 100.0

    @staticmethod
    def _base_label(symbol: str) -> str:
        text = (symbol or "").upper()
        suffixes = ("USDT", "USD", "USDC", "PERP", "-PERP", "_PERP")
        for suffix in suffixes:
            if text.endswith(suffix) and len(text) > len(suffix):
                text = text[: -len(suffix)]
                break
        return text or symbol


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
    spread_metrics: Optional[Dict[str, Any]] = None
    strategy_metrics: Optional[Dict[str, Any]] = None
    grvt_accounts: Optional[Dict[str, Any]] = None
    paradex_accounts: Optional[Dict[str, Any]] = None

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
        paradex_accounts_raw = payload.get("paradex_accounts")

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

        if isinstance(paradex_accounts_raw, dict):
            normalized_para = copy.deepcopy(paradex_accounts_raw)
            accounts_block = normalized_para.get("accounts")
            if isinstance(accounts_block, list) and len(accounts_block) > 50:
                normalized_para["accounts"] = accounts_block[:50]
            self.paradex_accounts = normalized_para

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
        if self.paradex_accounts is not None:
            payload["paradex_accounts"] = copy.deepcopy(self.paradex_accounts)
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
        self._bark_notifier: Optional[BarkNotifier] = None
        self._para_bark_notifier: Optional[BarkNotifier] = None
        self._risk_alert_threshold: Optional[float] = None
        self._risk_alert_reset: Optional[float] = None
        self._risk_alert_cooldown: float = 0.0

        self._para_risk_alert_threshold: Optional[float] = None
        self._para_risk_alert_reset: Optional[float] = None
        self._para_risk_alert_cooldown: float = 0.0

        self._para_stale_critical_seconds: float = 30.0
        self._apply_alert_settings()
        self._apply_para_alert_settings()
        self._risk_alert_active: Dict[str, bool] = {}
        self._risk_alert_last_ts: Dict[str, float] = {}

        self._para_risk_stats: Optional[ParaRiskSnapshot] = None
        self._global_risk_stats: Optional[GlobalRiskSnapshot] = None
        self._alert_history: Deque[Dict[str, Any]] = deque(maxlen=ALERT_HISTORY_LIMIT)
        self._alert_history_lock = asyncio.Lock()

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

    async def alert_settings_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return self._alert_settings_payload()

    async def para_alert_settings_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return self._para_alert_settings_payload()

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
        worst_loss_value = Decimal("0")
        worst_agent_id: Optional[str] = None
        worst_account_label: Optional[str] = None

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
                total_pnl = self._decimal_from(account_payload.get("total_pnl"))
                if total_pnl is None:
                    total_pnl = self._decimal_from(account_payload.get("total"))
                initial_margin = self._compute_initial_margin_total(account_payload)
                if max_initial_margin is None or initial_margin > max_initial_margin:
                    max_initial_margin = initial_margin
                if total_pnl is not None and total_pnl < 0:
                    abs_loss = abs(total_pnl)
                    if abs_loss > worst_loss_value:
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
            )

        ratio = float(worst_loss_value / risk_capacity)
        return ParaRiskSnapshot(
            ratio=ratio,
            risk_capacity=risk_capacity,
            worst_loss_value=worst_loss_value,
            worst_agent_id=worst_agent_id,
            worst_account_label=worst_account_label,
        )

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
        if self._bark_notifier is None or self._para_risk_alert_threshold is None:
            return []

        stats = self._para_risk_stats
        if stats is None or stats.ratio is None:
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

        if stats.ratio >= threshold and stats.risk_capacity > 0 and stats.worst_loss_value > 0:
            already_active = self._risk_alert_active.get(PARA_RISK_ALERT_KEY, False)
            last_ts = self._risk_alert_last_ts.get(PARA_RISK_ALERT_KEY, 0.0)
            cooldown = self._para_risk_alert_cooldown if self._para_risk_alert_cooldown else self._risk_alert_cooldown
            if not already_active and (now - last_ts) >= cooldown:
                alerts.append(
                    RiskAlertInfo(
                        key=PARA_RISK_ALERT_KEY,
                        agent_id=stats.worst_agent_id or agent_id,
                        account_label=stats.worst_account_label or "PARA",
                        ratio=stats.ratio,
                        loss_value=stats.worst_loss_value,
                        base_value=stats.risk_capacity,
                        base_label="Equity-1.5*max(IM)",
                    )
                )
                self._risk_alert_active[PARA_RISK_ALERT_KEY] = True
                self._risk_alert_last_ts[PARA_RISK_ALERT_KEY] = now
        else:
            if stats.ratio < reset_ratio:
                self._risk_alert_active.pop(PARA_RISK_ALERT_KEY, None)

        return alerts

    def _prepare_para_stale_alerts(self, now: float) -> Sequence[RiskAlertInfo]:
        if self._bark_notifier is None:
            return []
        critical = max(float(self._para_stale_critical_seconds or 0), 0.0)
        if critical <= 0:
            return []
        alerts: List[RiskAlertInfo] = []
        for agent_id, state in self._states.items():
            para_block = state.paradex_accounts
            if not isinstance(para_block, dict):
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
                continue
            age = now - ts
            if age < critical:
                continue
            key = f"para_stale::{agent_id}"
            last_ts = self._risk_alert_last_ts.get(key, 0.0)
            if (now - last_ts) < self._risk_alert_cooldown:
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
            self._risk_alert_last_ts[key] = now
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
        notifier = self._para_bark_notifier if alert.key == PARA_RISK_ALERT_KEY else self._bark_notifier
        if notifier is None:
            return
        # Bark 只需 URL，不传标题/正文
        title = ""
        body = ""
        status = "sent"
        error_message = None
        try:
            await notifier.send(title=title, body=body)
        except Exception as exc:  # pragma: no cover - network path
            status = "error"
            error_message = str(exc)
            LOGGER.warning("Bark alert delivery failed: %s", exc)
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
                if use_para and getattr(stats, "risk_capacity", None):
                    candidate = getattr(stats, "risk_capacity")
                elif (not use_para) and getattr(stats, "total_transferable", None):
                    candidate = getattr(stats, "total_transferable")
                if candidate:
                    base_value = candidate
            if base_value is None:
                base_value = Decimal("100000")
            loss_value_raw = overrides.get("loss_value")
            loss_value = self._decimal_from(loss_value_raw)
            if loss_value is None:
                try:
                    loss_value = Decimal(str(ratio)) * base_value
                except Exception:
                    loss_value = Decimal("50000")
            agent_id = (overrides.get("agent_id") or (stats.worst_agent_id if stats else None) or "test-agent")
            account_label = overrides.get("account_label") or (stats.worst_account_label if stats else None) or "Test Account"
            alert = RiskAlertInfo(
                key=PARA_RISK_ALERT_KEY if use_para else f"test::{agent_id}",
                agent_id=agent_id,
                account_label=account_label,
                ratio=ratio,
                loss_value=loss_value,
                base_value=base_value,
                base_label="Equity-1.5*max(IM)" if use_para else "Equity-IM",
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
        enable_volatility_monitor: bool = True,
        volatility_symbols: Optional[Sequence[str]] = None,
        volatility_poll_interval: float = 60.0,
        volatility_history_limit: int = 1800,
    ) -> None:
        self._coordinator = HedgeCoordinator(alert_settings=alert_settings)
        self._adjustments = GrvtAdjustmentManager()
        self._vol_monitor = (
            VolatilityMonitor(
                symbols=volatility_symbols,
                poll_interval=volatility_poll_interval,
                history_limit=volatility_history_limit,
            )
            if enable_volatility_monitor
            else None
        )
        self._price_service = HistoricalPriceFetcher()
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
        self._para_adjustments = GrvtAdjustmentManager()
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
                web.get("/risk_alert/settings", self.handle_risk_alert_settings),
                web.post("/risk_alert/settings", self.handle_risk_alert_update),
                web.get("/para/risk_alert/settings", self.handle_para_risk_alert_settings),
                web.post("/para/risk_alert/settings", self.handle_para_risk_alert_update),
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
                web.get("/simulation/pnl", self.handle_simulation_pnl),
            ]
        )
        if self._vol_monitor:
            self._app.on_startup.append(self._on_startup)
            self._app.on_cleanup.append(self._on_cleanup)

    @property
    def app(self) -> web.Application:
        return self._app

    async def _on_startup(self, app: web.Application) -> None:
        del app  # unused
        if self._vol_monitor:
            await self._vol_monitor.start()

    async def _on_cleanup(self, app: web.Application) -> None:
        del app  # unused
        if self._vol_monitor:
            await self._vol_monitor.stop()
        if self._price_service:
            await self._price_service.close()

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
        payload["para_adjustments"] = await self._para_adjustments.summary()
        payload["auto_balance"] = self._auto_balance_status_snapshot()
        payload["para_auto_balance"] = self._para_auto_balance_status_snapshot()
        if self._vol_monitor:
            payload["volatility"] = await self._vol_monitor.snapshot()
        return web.json_response(payload)

    async def handle_simulation_pnl(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        if not self._price_service:
            raise web.HTTPServiceUnavailable(text="historical price service unavailable")

        params = request.rel_url.query

        def _parse_int(name: str, default: int, *, min_value: int, max_value: int) -> int:
            raw = params.get(name)
            if raw is None:
                return default
            try:
                value = int(float(raw))
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text=f"{name} must be numeric")
            if value < min_value:
                value = min_value
            if value > max_value:
                value = max_value
            return value

        def _parse_float(name: str, default: float, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
            raw = params.get(name)
            if raw is None:
                return default
            try:
                value = float(str(raw))
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text=f"{name} must be numeric")
            if not math.isfinite(value):
                raise web.HTTPBadRequest(text=f"{name} must be finite")
            if min_value is not None and value < min_value:
                value = min_value
            if max_value is not None and value > max_value:
                value = max_value
            return value

        long_symbol = self._sanitize_binance_symbol(params.get("long_symbol"), "BTCUSDT")
        short_symbol = self._sanitize_binance_symbol(params.get("short_symbol"), "ETHUSDT")
        normalized_interval = self._price_service.normalize_interval(params.get("interval"))
        interval_minutes = self._price_service.interval_minutes(normalized_interval)
        window_days = _parse_int("window_days", 365, min_value=30, max_value=365)
        start_offset_days = _parse_float("start_offset_days", 0.0, min_value=0.0, max_value=float(window_days))
        samples_per_day = max(1, int(round(1440 / max(interval_minutes, 1))))
        target_points = max(samples_per_day * window_days, samples_per_day * 30)

        def _resolve_amount(primary: str, alternate: Optional[str], fallback: float) -> float:
            raw = params.get(primary)
            label = primary
            if raw is None and alternate:
                raw = params.get(alternate)
                if raw is not None:
                    label = alternate
            if raw is None:
                return fallback
            try:
                value = float(str(raw))
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text=f"{label} must be numeric")
            if not math.isfinite(value):
                raise web.HTTPBadRequest(text=f"{label} must be finite")
            value = abs(value)
            return value if value > 0 else fallback

        long_value = _resolve_amount("long_amount", "btc_amount", 100000.0)
        short_value = _resolve_amount("short_amount", "eth_amount", 150000.0)

        try:
            long_series = await self._price_service.get_series(long_symbol, interval=normalized_interval, points=target_points)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._raise_price_fetch_error(exc, long_symbol)

        try:
            short_series = await self._price_service.get_series(short_symbol, interval=normalized_interval, points=target_points)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._raise_price_fetch_error(exc, short_symbol)

        points = self._build_simulation_points(long_series, short_series)
        if len(points) < samples_per_day * 7:
            raise web.HTTPServiceUnavailable(text="insufficient overlapping price history")
        max_start_index = max(0, len(points) - 2)
        max_offset_days = max_start_index / max(samples_per_day, 1)
        start_offset_days = min(start_offset_days, max_offset_days)
        raw_start_index = int(start_offset_days * samples_per_day)
        start_index = min(max(0, raw_start_index), max_start_index)

        anchor_point = points[start_index] if start_index < len(points) else None
        base_long_price = float(anchor_point.get("long", 0.0)) if anchor_point else 0.0
        base_short_price = float(anchor_point.get("short", 0.0)) if anchor_point else 0.0

        def _notional_to_quantity(value: float, price: float) -> float:
            if value <= 0 or price <= 0:
                return 0.0
            return value / price

        long_amount = _notional_to_quantity(long_value, base_long_price)
        short_amount = _notional_to_quantity(short_value, base_short_price)

        pnl_series = self._compute_pair_pnl_series(points, start_index, long_amount, short_amount)
        start_anchor_ts = points[start_index]["ts"] if start_index < len(points) else None
        latest_pnl = pnl_series[-1]["pnl"] if pnl_series else 0.0
        range_info = self._build_range_payload(points)

        payload = {
            "source": "binance",
            "long_symbol": long_symbol,
            "short_symbol": short_symbol,
            "long_label": VolatilityMonitor._base_label(long_symbol),
            "short_label": VolatilityMonitor._base_label(short_symbol),
            "interval": normalized_interval,
            "window_days": window_days,
            "samples_per_day": samples_per_day,
            "point_count": len(points),
            "range": range_info,
            "default": {
                "start_index": start_index,
                "start_ts": start_anchor_ts,
                "start_iso": self._format_iso_timestamp(start_anchor_ts),
                "long_value": long_value,
                "short_value": short_value,
                "long_amount": long_amount,
                "short_amount": short_amount,
                "series": [
                    {"ts": row["ts"], "pnl": row["pnl"], "long": row["long"], "short": row["short"]}
                    for row in pnl_series
                ],
                "latest_pnl": latest_pnl,
            },
            "points": [
                {"ts": entry["ts"], "long": entry["long"], "short": entry["short"]}
                for entry in points
            ],
        }
        return web.json_response(payload)

    @staticmethod
    def _build_simulation_points(
        long_series: Sequence[Mapping[str, Any]],
        short_series: Sequence[Mapping[str, Any]],
    ) -> List[Dict[str, float]]:
        def _series_to_map(series: Sequence[Mapping[str, Any]]) -> Dict[int, float]:
            mapping: Dict[int, float] = {}
            for entry in series or []:
                ts_raw = entry.get("ts")
                price_raw = entry.get("close") if "close" in entry else entry.get("price")
                if ts_raw is None or price_raw is None:
                    continue
                try:
                    ts_value = int(round(float(ts_raw)))
                    price_value = float(price_raw)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(price_value):
                    continue
                mapping[ts_value] = price_value
            return mapping

        long_map = _series_to_map(long_series)
        short_map = _series_to_map(short_series)
        timestamps = sorted(set(long_map.keys()) & set(short_map.keys()))
        points: List[Dict[str, float]] = []
        for ts in timestamps:
            long_price = long_map.get(ts)
            short_price = short_map.get(ts)
            if long_price is None or short_price is None:
                continue
            points.append({
                "ts": float(ts),
                "long": float(long_price),
                "short": float(short_price),
            })
        return points

    @staticmethod
    def _compute_pair_pnl_series(
        points: Sequence[Mapping[str, float]],
        start_index: int,
        long_amount: float,
        short_amount: float,
    ) -> List[Dict[str, float]]:
        if not points:
            return []
        bounded_index = max(0, min(start_index, len(points) - 1))
        anchor = points[bounded_index]
        base_long = float(anchor.get("long", 0.0))
        base_short = float(anchor.get("short", 0.0))
        series: List[Dict[str, float]] = []
        for entry in points[bounded_index:]:
            try:
                ts = float(entry.get("ts", 0.0))
                long_price = float(entry.get("long", 0.0))
                short_price = float(entry.get("short", 0.0))
            except (TypeError, ValueError):
                continue
            pnl = (long_price - base_long) * long_amount - (short_price - base_short) * short_amount
            series.append({
                "ts": ts,
                "pnl": pnl,
                "long": long_price,
                "short": short_price,
            })
        return series

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

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="update payload must be an object")

        state = await self._coordinator.update(body)
        await self._maybe_auto_balance(state)
        await self._maybe_para_auto_balance(state)
        return web.json_response(state)

    async def handle_control_get(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        agent_id = request.rel_url.query.get("agent_id")
        snapshot = await self._coordinator.control_snapshot(agent_id)
        if agent_id:
            pending = []
            pending += await self._adjustments.pending_for_agent(agent_id)
            pending += await self._para_adjustments.pending_for_agent(agent_id)
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
            return web.json_response({
                "config": None,
                "status": self._para_auto_balance_status_snapshot(),
            })

        try:
            config = self._parse_auto_balance_config(body, default_currency="USDC")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))

        self._update_para_auto_balance_config(config)
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
        return web.json_response({"history": history})

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

    async def handle_risk_alert_test(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        if request.can_read_body and (request.content_length or 0) > 0:
            try:
                body = await request.json()
            except Exception:
                raise web.HTTPBadRequest(text="test payload must be JSON")
            if not isinstance(body, dict):
                raise web.HTTPBadRequest(text="test payload must be an object")
        else:
            body = {}
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
        try:
            payload = await self._coordinator.trigger_test_alert(overrides)
        except RuntimeError as exc:
            raise web.HTTPBadRequest(text=str(exc))
        LOGGER.info("Risk alert test triggered for Bark destination")
        return web.json_response({"alert": payload})

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
        order_mode_raw = str(body.get("order_mode") or "market").strip().lower()
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

    enable_vol_monitor = not getattr(args, "disable_volatility_monitor", False)
    volatility_symbols: Optional[List[str]] = None
    symbols_raw = getattr(args, "volatility_symbols", None)
    if symbols_raw:
        volatility_symbols = [item.strip() for item in symbols_raw.split(",") if item.strip()]
    coordinator_app = CoordinatorApp(
        dashboard_username=args.dashboard_username,
        dashboard_password=args.dashboard_password,
        dashboard_session_ttl=args.dashboard_session_ttl,
    alert_settings=alert_settings,
        enable_volatility_monitor=enable_vol_monitor,
        volatility_symbols=volatility_symbols,
        volatility_poll_interval=max(getattr(args, "volatility_poll_interval", 60.0), 20.0),
        volatility_history_limit=max(getattr(args, "volatility_history_limit", 1800), 100),
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
        default=_env_float("DASHBOARD_SESSION_TTL", 7 * 24 * 3600),
        help="Seconds that a dashboard login session remains valid (default 7 days).",
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
    parser.add_argument(
        "--disable-volatility-monitor",
        action="store_true",
        help="Disable external BTC/ETH volatility polling.",
    )
    parser.add_argument(
        "--volatility-symbols",
        default=os.getenv("VOLATILITY_SYMBOLS", "BTCUSDT,ETHUSDT"),
        help="Comma-separated spot symbols to sample for volatility (default BTCUSDT,ETHUSDT).",
    )
    parser.add_argument(
        "--volatility-poll-interval",
        type=float,
        default=_env_float("VOLATILITY_POLL_INTERVAL", 60.0),
        help="Seconds between volatility updates (min 20s).",
    )
    parser.add_argument(
        "--volatility-history-limit",
        type=int,
        default=int(os.getenv("VOLATILITY_HISTORY_LIMIT", "1800")),
        help="Number of 1m candles to keep for volatility stats (must exceed largest window).",
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
