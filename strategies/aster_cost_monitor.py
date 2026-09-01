"""Read-only Aster spread and execution-cost monitor.

This program never places orders. It samples public BBO data, computes rolling
spread and round-trip wear for a hypothetical 10,000 USD1 trade, and exposes a
loopback dashboard plus optional Feishu alerts when a configured condition is
met.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import dotenv
from aiohttp import web

LOGGER = logging.getLogger("strategies.aster_cost_monitor")
DEFAULT_REST_URL = "https://fapi.asterdex.com"
DEFAULT_SYMBOLS = ("SKHYNIXUSD1", "SPCXUSD1", "CLUSD1", "SNDKUSD1", "XAUUSD1", "MUUSD1")
DEFAULT_FEE_RATE = Decimal("0.00009")


def _decimal(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


def _json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(k): _json(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json(v) for v in value]
    return value


@dataclass(slots=True)
class CostSettings:
    symbols: Tuple[str, ...] = DEFAULT_SYMBOLS
    target_symbol: str = "XAUUSD1"
    rest_url: str = DEFAULT_REST_URL
    window_seconds: float = 900.0
    poll_seconds: float = 5.0
    notional: Decimal = Decimal("10000")
    fee_rate: Decimal = DEFAULT_FEE_RATE
    alert_enabled: bool = False
    alert_max_spread_bps: Decimal = Decimal("8")
    feishu_webhook_url: str = ""
    feishu_webhook_secret: str = ""
    feishu_interval_seconds: float = 600.0
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8792

    def validate(self) -> None:
        self.symbols = tuple(str(s).strip().upper() for s in self.symbols if str(s).strip())
        self.target_symbol = str(self.target_symbol).strip().upper()
        if not self.symbols or self.target_symbol not in self.symbols:
            raise ValueError("target_symbol must be included in symbols")
        for name, value in (("window_seconds", self.window_seconds), ("poll_seconds", self.poll_seconds), ("feishu_interval_seconds", self.feishu_interval_seconds)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.notional <= 0 or self.fee_rate < 0 or self.alert_max_spread_bps <= 0:
            raise ValueError("notional, fee rate, and alert spread must be valid")
        if self.dashboard_host not in {"127.0.0.1", "::1"}:
            raise ValueError("cost monitor dashboard must remain loopback-only")
        if self.feishu_webhook_url:
            parsed = urlparse(self.feishu_webhook_url)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError("Feishu webhook URL must use HTTPS")


@dataclass(frozen=True, slots=True)
class BBO:
    timestamp: float
    bid: Decimal
    ask: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid

    @property
    def spread_bps(self) -> Decimal:
        return self.spread / self.mid * Decimal("10000") if self.mid > 0 else Decimal("0")


@dataclass(slots=True)
class SymbolStats:
    symbol: str
    samples: Deque[BBO] = field(default_factory=deque)
    last_error: Optional[str] = None

    def add(self, point: BBO, cutoff: float) -> None:
        self.samples.append(point)
        while self.samples and self.samples[0].timestamp < cutoff:
            self.samples.popleft()
        self.last_error = None

    def as_payload(self, notional: Decimal, fee_rate: Decimal) -> Dict[str, Any]:
        if not self.samples:
            return {"symbol": self.symbol, "sample_count": 0, "error": self.last_error}
        latest = self.samples[-1]
        average_spread_bps = sum((item.spread_bps for item in self.samples), Decimal("0")) / Decimal(len(self.samples))
        average_spread = sum((item.spread for item in self.samples), Decimal("0")) / Decimal(len(self.samples))
        spread_cost = notional * average_spread_bps / Decimal("10000")
        round_trip_fees = notional * fee_rate * Decimal("2")
        return _json({
            "symbol": self.symbol,
            "sample_count": len(self.samples),
            "window_start": self.samples[0].timestamp,
            "window_end": latest.timestamp,
            "bid": latest.bid,
            "ask": latest.ask,
            "mid": latest.mid,
            "spread": latest.spread,
            "spread_bps": latest.spread_bps,
            "average_spread": average_spread,
            "average_spread_bps": average_spread_bps,
            "notional": notional,
            "fee_rate": fee_rate,
            "round_trip_fees": round_trip_fees,
            "spread_cost": spread_cost,
            "total_wear": spread_cost + round_trip_fees,
            "wear_bps": average_spread_bps + fee_rate * Decimal("2") * Decimal("10000"),
            "error": self.last_error,
        })


class AsterCostMonitor:
    def __init__(self, settings: CostSettings):
        settings.validate()
        self.settings = settings
        self.session: Optional[aiohttp.ClientSession] = None
        self.stats = {symbol: SymbolStats(symbol) for symbol in settings.symbols}
        self._dashboard_runner: Optional[web.AppRunner] = None
        self._dashboard_site: Optional[web.TCPSite] = None
        self._stop_event = asyncio.Event()
        self._next_feishu = 0.0
        self.last_error: Optional[str] = None

    async def _public_json(self, path: str, params: Mapping[str, Any]) -> Any:
        assert self.session is not None
        async with self.session.get(f"{self.settings.rest_url.rstrip('/')}{path}", params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
            text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"Aster public HTTP {response.status}: {text[:200]}")
            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError("Aster public response was not JSON") from exc

    async def sample_once(self) -> Dict[str, Any]:
        if self.session is None:
            self.session = aiohttp.ClientSession()
        cutoff = time.time() - self.settings.window_seconds
        for symbol, stats in self.stats.items():
            try:
                payload = await self._public_json("/fapi/v1/ticker/bookTicker", {"symbol": symbol})
                bid = _decimal(payload.get("bidPrice")) if isinstance(payload, Mapping) else None
                ask = _decimal(payload.get("askPrice")) if isinstance(payload, Mapping) else None
                if bid is None or ask is None or bid <= 0 or ask <= bid:
                    raise ValueError("invalid BBO")
                stats.add(BBO(time.time(), bid, ask), cutoff)
            except Exception as exc:
                stats.last_error = str(exc)
                self.last_error = f"{symbol}: {exc}"
                LOGGER.warning("Aster BBO sample failed for %s: %s", symbol, exc)
        return self.snapshot()

    def snapshot(self) -> Dict[str, Any]:
        symbols = {symbol: stats.as_payload(self.settings.notional, self.settings.fee_rate) for symbol, stats in self.stats.items()}
        target = symbols.get(self.settings.target_symbol, {})
        target_spread = _decimal(target.get("average_spread_bps"))
        condition = target_spread is not None and target_spread <= self.settings.alert_max_spread_bps
        return _json({
            "ok": any(item.get("sample_count", 0) > 0 for item in symbols.values()),
            "mode": "read_only_cost_monitor",
            "target_symbol": self.settings.target_symbol,
            "window_seconds": self.settings.window_seconds,
            "notional": self.settings.notional,
            "fee_rate": self.settings.fee_rate,
            "alert_enabled": self.settings.alert_enabled,
            "alert_max_spread_bps": self.settings.alert_max_spread_bps,
            "condition_met": condition,
            "last_error": self.last_error,
            "symbols": symbols,
            "updated_at": time.time(),
        })

    def _feishu_text(self) -> str:
        payload = self.snapshot()
        target = payload["symbols"].get(self.settings.target_symbol, {})
        lines = [
            "Aster 只读交易成本监控",
            time.strftime("时间: %Y-%m-%d %H:%M:%S %Z", time.localtime()),
            f"目标交易对: {self.settings.target_symbol}",
            f"平均买一卖一价差: {target.get('average_spread_bps', '-')} bps",
            f"每 10,000 名义金额预估磨损: {target.get('total_wear', '-')} USD1",
            f"其中双边手续费: {target.get('round_trip_fees', '-')} USD1（单边 {self.settings.fee_rate * 100}%）",
            f"监控状态: {'条件满足' if payload.get('condition_met') else '未满足'}",
        ]
        if payload.get("last_error"):
            lines.append(f"数据错误: {payload['last_error']}")
        return "\n".join(lines)

    async def _maybe_feishu(self) -> None:
        if not self.settings.alert_enabled or not self.settings.feishu_webhook_url or self.session is None:
            return
        payload = self.snapshot()
        if not payload.get("condition_met") and not payload.get("last_error"):
            return
        now = time.monotonic()
        if now < self._next_feishu:
            return
        self._next_feishu = now + self.settings.feishu_interval_seconds
        body: Dict[str, Any] = {"msg_type": "text", "content": {"text": self._feishu_text()}}
        if self.settings.feishu_webhook_secret:
            timestamp = str(int(time.time()))
            sign_source = f"{timestamp}\n{self.settings.feishu_webhook_secret}".encode()
            body.update({"timestamp": timestamp, "sign": base64.b64encode(hmac.new(self.settings.feishu_webhook_secret.encode(), sign_source, hashlib.sha256).digest()).decode()})
        try:
            async with self.session.post(self.settings.feishu_webhook_url, json=body, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    raise RuntimeError(f"Feishu HTTP {response.status}")
            LOGGER.info("Aster cost monitor Feishu alert sent")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("Aster cost monitor Feishu alert failed: %s", exc)

    async def start_dashboard(self) -> None:
        app = web.Application()
        app.router.add_get("/", self._dashboard_index)
        app.router.add_get("/api/snapshot", self._dashboard_snapshot)
        app.router.add_get("/api/healthz", self._dashboard_health)
        self._dashboard_runner = web.AppRunner(app, access_log=None)
        await self._dashboard_runner.setup()
        self._dashboard_site = web.TCPSite(self._dashboard_runner, self.settings.dashboard_host, self.settings.dashboard_port)
        await self._dashboard_site.start()

    async def _dashboard_index(self, _request: web.Request) -> web.Response:
        path = Path(__file__).with_name("aster_cost_monitor_dashboard.html")
        return web.Response(text=path.read_text(encoding="utf-8"), content_type="text/html")

    async def _dashboard_snapshot(self, _request: web.Request) -> web.Response:
        return web.json_response(self.snapshot())

    async def _dashboard_health(self, _request: web.Request) -> web.Response:
        payload = self.snapshot()
        return web.json_response({"ok": payload["ok"], "mode": payload["mode"]}, status=200 if payload["ok"] else 503)

    async def run(self) -> None:
        self.session = aiohttp.ClientSession()
        await self.start_dashboard()
        try:
            while not self._stop_event.is_set():
                await self.sample_once()
                await self._maybe_feishu()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.poll_seconds)
                except asyncio.TimeoutError:
                    pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._stop_event.set()
        if self._dashboard_runner is not None:
            with contextlib.suppress(Exception):
                await self._dashboard_runner.cleanup()
            self._dashboard_runner = None
        if self.session is not None:
            with contextlib.suppress(Exception):
                await self.session.close()
            self.session = None


def settings_from_env(env_file: Optional[str] = None) -> CostSettings:
    values: Dict[str, Any] = dict(os.environ)
    if env_file:
        values.update({key: value for key, value in dotenv.dotenv_values(env_file).items() if value is not None})
    symbols = tuple(item.strip().upper() for item in str(values.get("ASTER_COST_SYMBOLS", ",".join(DEFAULT_SYMBOLS))).split(",") if item.strip())
    return CostSettings(
        symbols=symbols,
        target_symbol=str(values.get("ASTER_COST_TARGET_SYMBOL", "XAUUSD1")),
        rest_url=str(values.get("ASTER_COST_REST_URL", REST_URL) or REST_URL).rstrip("/"),
        window_seconds=float(values.get("ASTER_COST_WINDOW_SECONDS", "900")),
        poll_seconds=float(values.get("ASTER_COST_POLL_SECONDS", "5")),
        notional=_required(values.get("ASTER_COST_NOTIONAL", "10000"), "notional"),
        fee_rate=_required(values.get("ASTER_COST_FEE_RATE", str(DEFAULT_FEE_RATE)), "fee rate"),
        alert_enabled=_env_bool(values.get("ASTER_COST_ALERT_ENABLED"), False),
        alert_max_spread_bps=_required(values.get("ASTER_COST_ALERT_MAX_SPREAD_BPS", "8"), "alert spread"),
        feishu_webhook_url=str(values.get("ASTER_COST_FEISHU_WEBHOOK_URL", "") or "").strip(),
        feishu_webhook_secret=str(values.get("ASTER_COST_FEISHU_WEBHOOK_SECRET", "") or "").strip(),
        feishu_interval_seconds=float(values.get("ASTER_COST_FEISHU_INTERVAL_SECONDS", "600")),
        dashboard_host=str(values.get("ASTER_COST_DASHBOARD_HOST", "127.0.0.1")),
        dashboard_port=int(values.get("ASTER_COST_DASHBOARD_PORT", "8792")),
    )


def _required(value: Any, label: str) -> Decimal:
    result = _decimal(value, None)
    if result is None:
        raise ValueError(f"{label} must be a finite decimal")
    return result


def _env_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().casefold() in {"1", "true", "yes", "on"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Aster spread and cost monitor")
    parser.add_argument("--env-file")
    parser.add_argument("--target-symbol")
    parser.add_argument("--window-seconds", type=float)
    parser.add_argument("--max-spread-bps", type=Decimal)
    parser.add_argument("--poll-seconds", type=float)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    settings = settings_from_env(args.env_file)
    for name, value in (("target_symbol", args.target_symbol), ("window_seconds", args.window_seconds), ("alert_max_spread_bps", args.max_spread_bps), ("poll_seconds", args.poll_seconds)):
        if value is not None:
            setattr(settings, name, value)
    settings.validate()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(AsterCostMonitor(settings).run())
    except KeyboardInterrupt:
        LOGGER.info("Aster cost monitor stopped")


if __name__ == "__main__":
    main()
