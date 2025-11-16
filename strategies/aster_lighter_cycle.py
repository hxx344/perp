#!/usr/bin/env python3
"""Aster–Lighter hedging cycle executor.

This module orchestrates a single four-leg hedging cycle:
    1. Aster maker entry.
    2. Lighter opposite taker fill.
    3. Aster reverse maker exit.
    4. Lighter reverse taker fill.

The implementation reuses the existing exchange adapters exposed via
``ExchangeFactory``. It can be invoked as a standalone script or imported as a
helper inside larger automation workflows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import subprocess
import sys
import time
import os
import gc
import tracemalloc
import math
import socket
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, cast
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp
from eth_account import Account
from web3 import Web3
from web3.exceptions import TimeExhausted
from web3.types import TxParams, Wei

from lighter.api.account_api import AccountApi
from lighter.api_client import ApiClient
from lighter.configuration import Configuration
from lighter.signer_client import SignerClient, create_api_key

import dotenv

from exchanges import ExchangeFactory
from exchanges.aster import AsterMarketDataWebSocket
from exchanges.base import OrderInfo
from helpers.logger import TradingLogger
from trading_bot import TradingConfig
try:  # pragma: no cover - package/script compatibility
    from .hedge_reporter import HedgeMetricsReporter
except ImportError:  # pragma: no cover - fallback when executed as a script
    from hedge_reporter import HedgeMetricsReporter

DEFAULT_ASTER_MAKER_DEPTH_LEVEL = 10
MIN_CYCLE_INTERVAL_SECONDS = 60.0

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from exchanges.aster import AsterClient
    from exchanges.lighter import LighterClient


class _BinanceFuturesPriceSource:
    """Lightweight market data client for Binance USDT-margined futures."""

    _BASE_URL = "https://fapi.binance.com"

    def __init__(self, symbol: str, logger: TradingLogger):
        self.symbol = symbol.upper()
        self.logger = logger
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        if params is None:
            params = {}

        url = f"{self._BASE_URL}{path}"
        timeout = aiohttp.ClientTimeout(total=5)
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=timeout)

        assert self._session is not None  # for type checkers
        async with self._session.get(url, params=params) as response:
            try:
                payload = await response.json()
            except aiohttp.ContentTypeError as exc:
                text = await response.text()
                raise RuntimeError(f"Binance response not JSON: {text}") from exc

            if response.status != 200:
                message = payload.get("msg") if isinstance(payload, dict) else payload
                raise RuntimeError(
                    f"Binance request {path} failed with status {response.status}: {message}"
                )

            return payload

    @staticmethod
    def _to_decimal_pairs(levels: list) -> List[Tuple[Decimal, Decimal]]:
        results: List[Tuple[Decimal, Decimal]] = []
        for item in levels:
            if not isinstance(item, list) or len(item) < 2:
                continue
            try:
                price = Decimal(item[0])
                quantity = Decimal(item[1])
            except (InvalidOperation, ValueError, TypeError):
                continue
            if price <= 0 or quantity < 0:
                continue
            results.append((price, quantity))
        return results

    @staticmethod
    def _select_depth_price(levels: List[Tuple[Decimal, Decimal]], level: int) -> Optional[Decimal]:
        if level <= 0:
            return None

        if level <= len(levels):
            return levels[level - 1][0]

        if levels:
            return levels[-1][0]

        return None

    async def fetch_order_book(self, limit: int = 50) -> Tuple[List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
        limit = max(5, min(limit, 500))
        payload = await self._get("/fapi/v1/depth", {"symbol": self.symbol, "limit": limit})

        bids = self._to_decimal_pairs(payload.get("bids", []))
        asks = self._to_decimal_pairs(payload.get("asks", []))
        return bids, asks

    async def fetch_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        payload = await self._get("/fapi/v1/ticker/bookTicker", {"symbol": self.symbol})

        try:
            bid = Decimal(payload.get("bidPrice", "0"))
        except (InvalidOperation, ValueError, TypeError):
            bid = Decimal("0")

        try:
            ask = Decimal(payload.get("askPrice", "0"))
        except (InvalidOperation, ValueError, TypeError):
            ask = Decimal("0")

        return bid, ask

    async def get_depth_level_price(
        self,
        direction: str,
        level: int,
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        bids, asks = await self.fetch_order_book(limit=max(level, 20))

        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None

        direction_lower = direction.lower()
        if direction_lower == "buy":
            depth_price = self._select_depth_price(bids, level)
        elif direction_lower == "sell":
            depth_price = self._select_depth_price(asks, level)
        else:
            depth_price = None

        return depth_price, best_bid, best_ask

    async def aclose(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()


class _AsterPublicDataClient:
    """Minimal Aster market data helper that avoids authenticated endpoints."""

    _BASE_URL = "https://fapi.asterdex.com"
    _WS_URL = "wss://fstream.asterdex.com"

    def __init__(self, ticker: str, logger: TradingLogger, depth_levels: int = 20) -> None:
        self.ticker = ticker.upper().strip()
        self.logger = logger
        self.depth_levels = max(5, min(depth_levels, 50))
        self.contract_id: Optional[str] = None
        self.tick_size: Decimal = Decimal("0")
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[AsterMarketDataWebSocket] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def initialize(self) -> Tuple[str, Decimal]:
        if not self.ticker:
            raise ValueError("Aster ticker must be provided for virtual mode")

        session = await self._ensure_session()
        async with session.get(f"{self._BASE_URL}/fapi/v1/exchangeInfo") as response:
            try:
                payload = await response.json()
            except aiohttp.ContentTypeError as exc:
                text = await response.text()
                raise RuntimeError(f"Aster exchangeInfo response not JSON: {text}") from exc

            if response.status != 200:
                message = payload if isinstance(payload, dict) else str(payload)
                raise RuntimeError(f"Failed to fetch Aster exchangeInfo ({response.status}): {message}")

        symbols = payload.get("symbols") if isinstance(payload, dict) else None
        if not symbols:
            raise RuntimeError("Aster exchangeInfo missing symbols array")

        for symbol_info in symbols:
            if not isinstance(symbol_info, dict):
                continue
            status = symbol_info.get("status")
            base_asset = symbol_info.get("baseAsset", "").upper()
            quote_asset = symbol_info.get("quoteAsset", "")
            if status != "TRADING" or base_asset != self.ticker or quote_asset != "USDT":
                continue

            contract_id = symbol_info.get("symbol", "")
            if not contract_id:
                continue

            tick_size = Decimal("0")
            for filter_info in symbol_info.get("filters", []):
                if isinstance(filter_info, dict) and filter_info.get("filterType") == "PRICE_FILTER":
                    tick_raw = str(filter_info.get("tickSize", "0")).strip()
                    try:
                        tick_size = Decimal(tick_raw)
                    except (InvalidOperation, ValueError):
                        tick_size = Decimal("0")
                    break

            if tick_size <= 0:
                raise RuntimeError(f"Failed to resolve tick size for Aster ticker {self.ticker}")

            self.contract_id = contract_id
            self.tick_size = tick_size
            await self._ensure_market_stream()
            return contract_id, tick_size

        raise RuntimeError(f"Unable to locate trading symbol for Aster ticker {self.ticker}")

    async def _ensure_market_stream(self) -> bool:
        if not self.contract_id:
            return False

        symbol = self.contract_id.lower()
        try:
            if self._ws is None or self._ws.symbol != symbol:
                if self._ws is not None:
                    await self._ws.stop()
                self._ws = AsterMarketDataWebSocket(
                    symbol=symbol,
                    base_ws_url=self._WS_URL,
                    logger=self.logger,
                    depth_levels=self.depth_levels,
                )
                await self._ws.start()

            if self._ws:
                return await self._ws.wait_until_ready(timeout=2.0)
        except Exception as exc:
            if self.logger:
                self.logger.log(f"Failed to start Aster public market stream: {exc}", "WARNING")
        return False

    async def fetch_order_book(self, limit: int = 20) -> Tuple[List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
        limit = max(5, min(limit, 500))
        ws_ready = await self._ensure_market_stream()
        if ws_ready and self._ws is not None:
            bids, asks = await self._ws.get_depth(limit)
            if bids and asks:
                return bids, asks

        if not self.contract_id:
            raise RuntimeError("Aster contract id not initialized")

        session = await self._ensure_session()
        params = {"symbol": self.contract_id, "limit": str(limit)}
        async with session.get(f"{self._BASE_URL}/fapi/v1/depth", params=params) as response:
            try:
                payload = await response.json()
            except aiohttp.ContentTypeError as exc:
                text = await response.text()
                raise RuntimeError(f"Aster depth response not JSON: {text}") from exc

            if response.status != 200:
                raise RuntimeError(f"Failed to fetch Aster depth ({response.status}): {payload}")

        bids_raw = payload.get("bids", []) if isinstance(payload, dict) else []
        asks_raw = payload.get("asks", []) if isinstance(payload, dict) else []

        def _parse(levels: List[List[str]]) -> List[Tuple[Decimal, Decimal]]:
            parsed: List[Tuple[Decimal, Decimal]] = []
            for entry in levels:
                if not isinstance(entry, list) or len(entry) < 2:
                    continue
                price_raw, qty_raw = entry[0], entry[1]
                try:
                    price = Decimal(str(price_raw))
                    qty = Decimal(str(qty_raw))
                except (InvalidOperation, ValueError, TypeError):
                    continue
                if price <= 0 or qty < 0:
                    continue
                parsed.append((price, qty))
                if len(parsed) >= limit:
                    break
            return parsed

        return _parse(bids_raw), _parse(asks_raw)

    async def fetch_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        ws_ready = await self._ensure_market_stream()
        if ws_ready and self._ws is not None:
            bid, ask = await self._ws.get_bbo()
            if bid and bid > 0 and ask and ask > 0:
                return bid, ask

        if not self.contract_id:
            raise RuntimeError("Aster contract id not initialized")

        session = await self._ensure_session()
        params = {"symbol": self.contract_id}
        async with session.get(f"{self._BASE_URL}/fapi/v1/ticker/bookTicker", params=params) as response:
            try:
                payload = await response.json()
            except aiohttp.ContentTypeError as exc:
                text = await response.text()
                raise RuntimeError(f"Aster bookTicker response not JSON: {text}") from exc

            if response.status != 200:
                raise RuntimeError(f"Failed to fetch Aster bookTicker ({response.status}): {payload}")

        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected Aster bookTicker payload: {payload}")

        try:
            bid = Decimal(payload.get("bidPrice", "0"))
        except (InvalidOperation, ValueError, TypeError):
            bid = Decimal("0")
        try:
            ask = Decimal(payload.get("askPrice", "0"))
        except (InvalidOperation, ValueError, TypeError):
            ask = Decimal("0")

        return bid, ask

    async def get_depth_level_price(self, direction: str, level: int) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        bids, asks = await self.fetch_order_book(limit=max(level, 20))

        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None

        def _select(levels: List[Tuple[Decimal, Decimal]]) -> Optional[Decimal]:
            if level <= 0 or not levels:
                return None
            index = level - 1
            if 0 <= index < len(levels):
                return levels[index][0]
            return levels[-1][0]

        direction_lower = direction.lower()
        if direction_lower == "buy":
            depth_price = _select(bids)
        elif direction_lower == "sell":
            depth_price = _select(asks)
        else:
            depth_price = None

        return depth_price, best_bid, best_ask

    async def aclose(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.stop()
            except Exception:
                pass
            self._ws = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

def _decimal_type(value: str) -> Decimal:
    """Argparse helper that parses Decimal values with validation."""
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - defensive
        raise argparse.ArgumentTypeError(f"Invalid decimal value: {value}") from exc


@dataclass
class CycleConfig:
    """User-configurable parameters for the hedging cycle."""

    aster_ticker: str
    lighter_ticker: str
    quantity: Decimal
    aster_quantity: Decimal
    lighter_quantity: Decimal
    direction: str
    take_profit_pct: Decimal  # retained for compatibility; reverse leg now ignores this value
    slippage_pct: Decimal
    max_wait_seconds: float
    lighter_max_wait_seconds: float
    poll_interval: float
    max_retries: int  # <=0 disables retry limit
    retry_delay_seconds: float
    max_cycles: int
    delay_between_cycles: float
    virtual_aster_maker: bool
    randomize_direction: bool = False
    direction_seed: Optional[int] = None
    lighter_quantity_min: Optional[Decimal] = None
    lighter_quantity_max: Optional[Decimal] = None
    preserve_initial_position: bool = False
    coordinator_url: Optional[str] = None
    coordinator_agent_id: Optional[str] = None
    coordinator_pause_poll_seconds: float = 5.0
    coordinator_username: Optional[str] = None
    coordinator_password: Optional[str] = None
    virtual_aster_price_source: str = "aster"
    virtual_aster_reference_symbol: Optional[str] = None
    aster_maker_depth_level: int = DEFAULT_ASTER_MAKER_DEPTH_LEVEL
    aster_leg1_depth_level: Optional[int] = None
    aster_leg3_depth_level: Optional[int] = None
    hot_update_url: Optional[str] = None
    # Housekeeping and memory monitoring
    memory_clean_interval_seconds: float = 300.0
    memory_warn_mb: float = 0.0
    # Logging
    log_to_console: bool = True
    # Tracemalloc sampling for memory hotspot diagnostics
    tracemalloc_enabled: bool = False
    tracemalloc_top: int = 15
    tracemalloc_group_by: str = "lineno"  # one of: lineno, traceback, filename
    tracemalloc_filter: Optional[str] = None  # only include entries whose path/trace contains this substring
    tracemalloc_frames: int = 25
    enforce_min_cycle_interval: bool = True
    lighter_leverage: int = 50


@dataclass
class LegResult:
    """Execution summary for a single leg in the cycle."""

    name: str
    exchange: str
    side: str
    quantity: Decimal
    price: Decimal
    order_id: str
    status: str
    latency_seconds: float
    requested_price: Optional[Decimal] = None
    reference_price: Optional[Decimal] = None


class SkipCycleError(Exception):
    """Raised when the current hedging cycle should be skipped."""

    pass


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    """Round a price to the nearest tick size."""
    if tick <= 0:
        return value
    return value.quantize(tick, rounding=ROUND_HALF_UP)


def _format_duration(seconds: float) -> str:
    """Represent a duration in days/hours/minutes/seconds."""
    if seconds < 0:
        seconds = 0.0

    whole_seconds = int(seconds)
    fractional = seconds - whole_seconds

    days, remainder = divmod(whole_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    secs = secs + fractional

    return f"{days}d {hours}h {minutes}m {secs:.2f}s"


def _format_decimal(value: Optional[Decimal], places: int = 6) -> str:
    if value is None:
        return "N/A"

    quantize_exp = Decimal(1).scaleb(-places)
    try:
        quantized = value.quantize(quantize_exp, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return str(value)

    text = f"{quantized:f}"
    if "e" in text.lower():
        text = format(quantized, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


def _compute_cycle_pause_seconds(
    cycle_start_time: float,
    configured_delay: float,
    *,
    enforce_min_interval: bool = True,
) -> float:
    elapsed = max(0.0, time.time() - cycle_start_time)
    if enforce_min_interval:
        remaining_for_min = max(0.0, MIN_CYCLE_INTERVAL_SECONDS - elapsed)
        return float(max(configured_delay, remaining_for_min))

    return float(max(0.0, configured_delay))


_LEADERBOARD_ENDPOINT = "/api/v1/leaderboard"
_LEADERBOARD_REFRESH_CYCLES = 100

_HOT_UPDATE_SWITCH_KEYS = (
    "cycle_enabled",
    "hedge_enabled",
    "hedging_enabled",
    "enable_cycles",
)
_HOT_UPDATE_SWITCH_RECHECK_SECONDS = 60.0


def _coerce_optional_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float) and math.isnan(value):
            return None
        if value == 0:
            return False
        if value == 1:
            return True
        return None

    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"1", "true", "yes", "on", "enable", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disable", "disabled"}:
            return False

    return None


def _extract_cycle_switch(payload: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not isinstance(payload, dict):
        return None

    for key in _HOT_UPDATE_SWITCH_KEYS:
        if key in payload:
            coerced = _coerce_optional_bool(payload.get(key))
            if coerced is not None:
                return coerced

    nested = payload.get("controls")
    if isinstance(nested, dict):
        for key in _HOT_UPDATE_SWITCH_KEYS:
            if key in nested:
                coerced = _coerce_optional_bool(nested.get(key))
                if coerced is not None:
                    return coerced

    return None


def _extract_leaderboard_points(entries: Any, target_address: str) -> Optional[Decimal]:
    if not isinstance(entries, list):
        return None

    candidate: Optional[Dict[str, Any]] = None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_id = entry.get("entryId") or entry.get("entry_id")
        if entry_id == 11:
            candidate = entry
            break

    if candidate is None and target_address:
        target_lower = target_address.lower()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            address = str(entry.get("l1_address") or entry.get("address") or "").lower()
            if address == target_lower:
                candidate = entry
                break

    if candidate is None:
        return None

    points_raw = candidate.get("points")
    if points_raw is None:
        return None

    try:
        return Decimal(str(points_raw))
    except (InvalidOperation, ValueError, TypeError):
        return None


async def _fetch_lighter_leaderboard_points(
    l1_address: str,
    *,
    base_url: Optional[str] = None,
    timeout_seconds: float = 10.0,
    auth_token: Optional[str] = None,
) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    address = (l1_address or "").strip()
    if not address:
        return None, None

    leaderboard_base = (base_url or os.getenv("LIGHTER_BASE_URL") or LIGHTER_MAINNET_BASE_URL).strip()
    if not leaderboard_base:
        leaderboard_base = LIGHTER_MAINNET_BASE_URL

    leaderboard_base = leaderboard_base.rstrip("/")
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    session = aiohttp.ClientSession(timeout=timeout)
    close_session = True

    async def _request_points(board_type: str) -> Optional[Decimal]:
        url = f"{leaderboard_base}{_LEADERBOARD_ENDPOINT}"
        params = {"type": board_type, "l1_address": address}
        if auth_token:
            params["auth"] = auth_token
        headers = {}
        if auth_token:
            headers["Authorization"] = auth_token
        async with session.get(url, params=params, headers=headers or None) as response:
            body_text = await response.text()
            try:
                payload = await response.json(content_type=None)
            except aiohttp.ContentTypeError as exc:
                raise RuntimeError(
                    f"Leaderboard response for type '{board_type}' is not JSON: {body_text}"
                ) from exc

            if response.status != 200:
                raise RuntimeError(
                    f"Leaderboard request for type '{board_type}' failed with HTTP {response.status}: {body_text}"
                )

            entries = payload.get("entries") if isinstance(payload, dict) else None
            return _extract_leaderboard_points(entries, address)

    try:
        weekly_points = await _request_points("weekly")
        total_points = await _request_points("all")
        return weekly_points, total_points
    finally:
        if close_session and not session.closed:
            await session.close()


LIGHTER_MAINNET_BASE_URL = "https://mainnet.zklighter.elliot.ai"
ARBITRUM_CHAIN_ID = 42161
# Native USDC on Arbitrum One (deployed by Circle, not the legacy bridged USDC.e)


def _fetch_hot_update_payload(url: Optional[str], logger: Optional[TradingLogger]) -> Optional[Dict[str, Any]]:
    if not url:
        return None

    trimmed = url.strip()
    if not trimmed:
        return None

    cache_bust_url = trimmed
    try:
        parsed = urlparse(trimmed)
        if parsed.scheme in {"http", "https"}:
            query_items = parse_qsl(parsed.query, keep_blank_values=True)
            query_items.append(("_", str(int(time.time()))))
            cache_bust_query = urlencode(query_items)
            cache_bust_url = urlunparse(parsed._replace(query=cache_bust_query))
    except Exception:
        cache_bust_url = trimmed

    try:
        result = subprocess.run(
            ["curl", "-fsSL", cache_bust_url],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        if logger:
            logger.log(f"curl command not found for hot update fetch: {exc}", "ERROR")
        return None
    except subprocess.CalledProcessError as exc:
        if logger:
            logger.log(
                f"Failed to download hot update config from {trimmed}: {exc.stderr.strip() or exc}",
                "WARNING",
            )
        return None

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        if logger:
            logger.log(
                f"Invalid JSON in hot update config from {trimmed}: {exc}",
                "WARNING",
            )
        return None

    if not isinstance(payload, dict):
        if logger:
            logger.log(
                f"Hot update config from {trimmed} is not an object: {type(payload).__name__}",
                "WARNING",
            )
        return None

    return payload


async def _wait_for_hot_update_enabled(
    hot_update_url: str,
    logger: TradingLogger,
    *,
    sleep_seconds: float = _HOT_UPDATE_SWITCH_RECHECK_SECONDS,
) -> Optional[Dict[str, Any]]:
    if not hot_update_url:
        return None

    while True:
        payload = _fetch_hot_update_payload(hot_update_url, logger)

        if payload is None:
            logger.log(
                "Hot update payload unavailable; proceeding without switch enforcement",
                "WARNING",
            )
            return None

        switch_state = _extract_cycle_switch(payload)
        if switch_state is False:
            logger.log(
                (
                    "Hot update cycle switch disabled; pausing %.0f seconds before rechecking"
                    % sleep_seconds
                ),
                "WARNING",
            )
            await asyncio.sleep(max(0.0, sleep_seconds))
            continue

        return payload

    return payload
ARBITRUM_USDC_CONTRACT = Web3.to_checksum_address("0xaf88d065e77c8cc2239327c5edb3a432268e5831")
_REQUIRED_LIGHTER_ENV = (
    "API_KEY_PRIVATE_KEY",
    "LIGHTER_ACCOUNT_INDEX",
    "LIGHTER_API_KEY_INDEX",
)
_ENV_KEYS_TO_CLEAR = (
    "L1_WALLET_PRIVATE_KEY",
    "LIGHTER_L1_PRIVATE_KEY",
    "API_KEY_PRIVATE_KEY",
    "LIGHTER_ACCOUNT_INDEX",
    "LIGHTER_API_KEY_INDEX",
    "L1_WALLET_ADDRESS",
)
DEFAULT_LIGHTER_LEVERAGE = 50
L1_USDC_TOPUP_THRESHOLD = Decimal("1")

_ERC20_TRANSFER_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [
            {"name": "recipient", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
]


class LighterProvisioningError(RuntimeError):
    """Raised when automated provisioning for Lighter credentials fails."""


def _persist_env_value(env_path: Path, key: str, value: str) -> None:
    normalized = value.strip()
    dotenv.set_key(str(env_path), key, normalized, quote_mode="never")
    os.environ[key] = normalized


def _normalize_private_key_hex(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return cleaned
    return cleaned if cleaned.startswith("0x") else f"0x{cleaned}"


def _clear_env_credentials(env_path: Path, *, logger: Optional[logging.Logger] = None) -> bool:
    removed_any = False
    if not env_path.exists():
        if logger:
            logger.warning("Env file %s does not exist; nothing to clear", env_path)
        return False

    for key in _ENV_KEYS_TO_CLEAR:
        try:
            previous_value = dotenv.get_key(str(env_path), key)
        except Exception:
            previous_value = None
        try:
            result = dotenv.set_key(str(env_path), key, "", quote_mode="never")
        except Exception as exc:
            if logger:
                logger.warning("Failed to clear %s in %s: %s", key, env_path, exc)
            continue

        os.environ[key] = ""

        success = bool(result)

        if success:
            if logger:
                logger.info("Cleared %s in %s", key, env_path)
            if previous_value not in (None, ""):
                removed_any = True
        elif logger:
            logger.info("Ensured blank value for %s in %s", key, env_path)

    return removed_any


async def _request_lighter_intent_address(
    base_url: str,
    from_address: str,
    *,
    chain_id: int = ARBITRUM_CHAIN_ID,
    session: Optional[aiohttp.ClientSession] = None,
) -> str:
    close_session = False
    if session is None or session.closed:
        timeout = aiohttp.ClientTimeout(total=15)
        session = aiohttp.ClientSession(timeout=timeout)
        close_session = True

    assert session is not None

    try:
        url = f"{base_url.rstrip('/')}/api/v1/createIntentAddress"
        form_payload = {
            "from_addr": from_address,
            "is_external_deposit": "true",
            "amount": "0",
            "chain_id": str(chain_id),
        }
        async with session.post(url, data=form_payload) as response:
            body_text = await response.text()
            try:
                payload = await response.json(content_type=None)
            except aiohttp.ContentTypeError:
                payload = None

            if response.status != 200:
                raise LighterProvisioningError(
                    f"createIntentAddress failed with HTTP {response.status}: {body_text}"
                )

            if isinstance(payload, dict):
                intent_address = (
                    payload.get("intent_address")
                    or payload.get("deposit_address")
                    or payload.get("address")
                )
                if intent_address:
                    return Web3.to_checksum_address(intent_address)

            raise LighterProvisioningError(
                f"createIntentAddress response missing deposit address: {payload or body_text}"
            )
    finally:
        if close_session and session is not None:
            await session.close()


async def _fetch_lighter_account_overview(
    base_url: str,
    l1_address: str,
    *,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[Dict[str, Any]]:
    close_session = False
    if session is None or session.closed:
        timeout = aiohttp.ClientTimeout(total=15)
        session = aiohttp.ClientSession(timeout=timeout)
        close_session = True

    assert session is not None

    try:
        url = f"{base_url.rstrip('/')}/api/v1/account"
        params = {"by": "l1_address", "value": l1_address}
        async with session.get(url, params=params) as response:
            body_text = await response.text()
            payload: Any
            try:
                payload = await response.json(content_type=None)
            except aiohttp.ContentTypeError:
                payload = None

            if response.status != 200:
                raise LighterProvisioningError(
                    f"account query failed with HTTP {response.status}: {body_text}"
                )

            accounts_raw: Any = None
            if isinstance(payload, dict):
                accounts_raw = payload.get("accounts")

            if isinstance(accounts_raw, list):
                for item in accounts_raw:
                    if isinstance(item, dict):
                        return item
            return None
    finally:
        if close_session and session is not None:
            await session.close()


def _build_usdc_contract(web3: Web3):
    return web3.eth.contract(address=ARBITRUM_USDC_CONTRACT, abi=_ERC20_TRANSFER_ABI)


def _send_full_usdc_transfer(
    web3: Web3,
    private_key: str,
    from_address: str,
    destination: str,
) -> Tuple[str, int, int]:
    checksum_from = Web3.to_checksum_address(from_address)
    checksum_dest = Web3.to_checksum_address(destination)
    contract = _build_usdc_contract(web3)

    balance = contract.functions.balanceOf(checksum_from).call()
    if balance <= 0:
        raise LighterProvisioningError(
            "Arbitrum wallet native USDC (0xaf88...) balance is zero; cannot fund Lighter account"
        )

    decimals = int(contract.functions.decimals().call())

    nonce = web3.eth.get_transaction_count(checksum_from)
    gas_price = web3.eth.gas_price
    buffered_gas_price: Wei = cast(Wei, max(gas_price * 2, gas_price + 1))
    transfer_fn = contract.functions.transfer(checksum_dest, balance)
    gas_estimate = transfer_fn.estimate_gas({"from": checksum_from})
    tx_params: TxParams = {
        "from": checksum_from,
        "nonce": nonce,
        "gas": gas_estimate,
        "gasPrice": buffered_gas_price,
        "chainId": ARBITRUM_CHAIN_ID,
    }
    tx = transfer_fn.build_transaction(tx_params)

    signed_tx = web3.eth.account.sign_transaction(tx, private_key=private_key)
    raw_tx = getattr(signed_tx, "rawTransaction", None)
    if raw_tx is None:
        raw_tx = getattr(signed_tx, "raw_transaction", None)
    if raw_tx is None:
        raise LighterProvisioningError("Signed transaction missing raw transaction payload")
    tx_hash = web3.eth.send_raw_transaction(raw_tx)
    try:
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=600)
    except TimeExhausted as exc:
        raise LighterProvisioningError(
            f"USDC transfer transaction {tx_hash.hex()} not confirmed within timeout"
        ) from exc

    if getattr(receipt, "status", 0) != 1:
        raise LighterProvisioningError(
            f"USDC transfer transaction {tx_hash.hex()} failed with status {getattr(receipt, 'status', 'unknown')}"
        )

    return tx_hash.hex(), balance, decimals


async def _transfer_full_usdc_balance(
    web3: Web3,
    private_key: str,
    from_address: str,
    destination: str,
) -> Tuple[str, int, int]:
    return await asyncio.to_thread(
        _send_full_usdc_transfer,
        web3,
        private_key,
        from_address,
        destination,
    )


def _format_usdc_amount_raw(amount: int, decimals: int) -> str:
    quant = Decimal(amount) / (Decimal(10) ** Decimal(decimals))
    return f"{quant.normalize():f} USDC" if quant else "0 USDC"


def _extract_available_balance(account_obj: object) -> Decimal:
    candidate = getattr(account_obj, "available_balance", None)
    if candidate is None:
        candidate = getattr(account_obj, "availableBalance", None)
    if candidate is None and isinstance(account_obj, dict):
        candidate = account_obj.get("available_balance") or account_obj.get("availableBalance")
    to_dict_fn = getattr(account_obj, "to_dict", None)
    if candidate is None and callable(to_dict_fn):
        try:
            account_dict_raw = to_dict_fn()
        except Exception:
            account_dict_raw = None
        account_dict: Dict[str, Any] = {}
        if isinstance(account_dict_raw, dict):
            account_dict = cast(Dict[str, Any], account_dict_raw)
        candidate = account_dict.get("available_balance") or account_dict.get("availableBalance")

    if candidate is None:
        return Decimal("0")

    try:
        return Decimal(str(candidate))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


async def _wait_for_lighter_account_ready(
    base_url: str,
    l1_address: str,
    *,
    expected_balance: Optional[Decimal],
    logger: logging.Logger,
    timeout_seconds: float = 900.0,
    poll_interval: float = 60.0,
) -> Tuple[int, Decimal]:
    deadline = time.time() + timeout_seconds
    tolerance = Decimal("0.000001")
    if expected_balance is not None and expected_balance > 0:
        tolerance = max(tolerance, expected_balance * Decimal("0.000001"))

    last_error: Optional[str] = None
    timeout = aiohttp.ClientTimeout(total=15)
    session = aiohttp.ClientSession(timeout=timeout)

    try:
        while time.time() < deadline:
            try:
                account_data = await _fetch_lighter_account_overview(
                    base_url,
                    l1_address,
                    session=session,
                )
            except LighterProvisioningError as exc:
                last_error = str(exc)
                logger.warning("Account status request failed: %s", exc)
                await asyncio.sleep(poll_interval)
                continue
            except Exception as exc:
                last_error = str(exc)
                await asyncio.sleep(poll_interval)
                continue

            if not account_data:
                last_error = "account not found"
                await asyncio.sleep(poll_interval)
                continue

            account_index_raw = account_data.get("account_index") or account_data.get("index")
            account_index: Optional[int] = None
            if account_index_raw is not None:
                try:
                    account_index = int(account_index_raw)
                except (TypeError, ValueError):
                    account_index = None

            if account_index is None:
                last_error = "invalid account index"
                await asyncio.sleep(poll_interval)
                continue

            available_balance = _extract_available_balance(account_data)
            if expected_balance is None:
                if available_balance > Decimal("0"):
                    return account_index, available_balance
                last_error = "available balance still zero"
            else:
                if available_balance >= expected_balance - tolerance:
                    return account_index, available_balance
                logger.info(
                    "Lighter balance %s below expected %s; retrying in %.1fs",
                    f"{available_balance.normalize():f}",
                    f"{expected_balance.normalize():f}",
                    poll_interval,
                )

            await asyncio.sleep(poll_interval)

        raise LighterProvisioningError(
            f"Timed out waiting for Lighter balance for {l1_address}: {last_error or 'balance not reached'}"
        )
    finally:
        if not session.closed:
            await session.close()


async def _auto_provision_lighter_credentials(env_path: Path) -> None:
    if all(os.getenv(key) for key in _REQUIRED_LIGHTER_ENV):
        return

    logger = logging.getLogger("lighter.provisioning")
    logger.info("Missing Lighter credentials detected; starting auto provisioning")

    base_url = (os.getenv("LIGHTER_BASE_URL") or LIGHTER_MAINNET_BASE_URL).strip()
    if not base_url:
        base_url = LIGHTER_MAINNET_BASE_URL

    if "mainnet" not in base_url.lower():
        raise LighterProvisioningError("Auto provisioning currently supports only Lighter mainnet")

    rpc_url = (
        os.getenv("ARBITRUM_RPC_URL")
        or os.getenv("L1_RPC_URL")
        or os.getenv("ARBITRUM_ONE_RPC_URL")
    )
    if not rpc_url:
        raise LighterProvisioningError(
            "ARBITRUM_RPC_URL (or L1_RPC_URL / ARBITRUM_ONE_RPC_URL) must be configured"
        )

    l1_private_key_env = (
        os.getenv("L1_WALLET_PRIVATE_KEY")
        or os.getenv("LIGHTER_L1_PRIVATE_KEY")
        or ""
    )
    if not l1_private_key_env:
        raise LighterProvisioningError("L1_WALLET_PRIVATE_KEY is required for provisioning")

    l1_private_key = _normalize_private_key_hex(l1_private_key_env)
    _persist_env_value(env_path, "L1_WALLET_PRIVATE_KEY", l1_private_key)

    account = Account.from_key(l1_private_key)
    l1_address = Web3.to_checksum_address(account.address)
    _persist_env_value(env_path, "L1_WALLET_ADDRESS", l1_address)

    web3 = Web3(Web3.HTTPProvider(rpc_url))
    if not web3.is_connected():
        raise LighterProvisioningError(f"Unable to connect to Arbitrum RPC at {rpc_url}")

    try:
        existing_account = await _fetch_lighter_account_overview(base_url, l1_address)
    except Exception as exc:
        logger.warning("Failed to query existing Lighter account: %s", exc)
        existing_account = None

    existing_balance = Decimal("0")
    if existing_account is not None:
        existing_balance = _extract_available_balance(existing_account)

    expected_balance: Optional[Decimal] = None
    if existing_balance > Decimal("0"):
        expected_balance = existing_balance
        logger.info(
            "Detected existing Lighter balance %s; skipping USDC transfer",
            f"{existing_balance.normalize():f}",
        )
    else:
        logger.info("Requesting Lighter intent deposit address for %s", l1_address)
        intent_address = await _request_lighter_intent_address(base_url, l1_address)
        logger.info("Received Lighter intent address %s", intent_address)

        logger.info("Transferring full USDC balance from %s to intent address", l1_address)
        tx_hash, raw_amount, decimals = await _transfer_full_usdc_balance(
            web3,
            l1_private_key,
            l1_address,
            intent_address,
        )
        transfer_amount = Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))
        expected_balance = existing_balance + transfer_amount

        logger.info(
            "Submitted USDC bridge transaction %s for %s",
            tx_hash,
            _format_usdc_amount_raw(raw_amount, decimals),
        )

    logger.info("Waiting for Lighter account readiness")
    account_index, credited_balance = await _wait_for_lighter_account_ready(
        base_url,
        l1_address,
        expected_balance=expected_balance,
        logger=logger,
    )

    balance_display = f"{credited_balance.normalize():f}" if credited_balance else "0"
    if expected_balance is not None:
        logger.info(
            "Lighter account ready: balance=%s (expected≈%s) account_index=%s",
            balance_display,
            f"{expected_balance.normalize():f}",
            account_index,
        )
    else:
        logger.info(
            "Lighter account ready: balance=%s (account index %s)",
            balance_display,
            account_index,
        )

    logger.info("Generating new Lighter API key pair")
    private_key, public_key, err = create_api_key()
    if err:
        raise LighterProvisioningError(f"Failed to create Lighter API key: {err}")

    if not isinstance(private_key, str) or not isinstance(public_key, str):
        raise LighterProvisioningError("Lighter SDK returned invalid API key pair")

    private_key = _normalize_private_key_hex(private_key)
    public_key = _normalize_private_key_hex(public_key)

    api_key_index = random.randint(2, 253)
    logger.info("Assigning API key index %s", api_key_index)

    signer_client = SignerClient(
        url=base_url,
        private_key=private_key,
        account_index=account_index,
        api_key_index=api_key_index,
    )
    try:
        logger.info("Submitting change_api_key transaction to bind API key")
        _, error = await signer_client.change_api_key(
            eth_private_key=l1_private_key,
            new_pubkey=public_key,
        )
        if error is not None:
            raise LighterProvisioningError(f"Failed to bind API key: {error}")

        logger.info("Waiting for API key propagation on Lighter")
        deadline = time.time() + 120
        wait_error: Optional[str] = None
        while time.time() < deadline:
            check_error = signer_client.check_client()
            if check_error is None:
                break
            wait_error = check_error
            await asyncio.sleep(6)
        else:
            raise LighterProvisioningError(
                f"API key verification failed: {wait_error or 'unknown error'}"
            )
    finally:
        await signer_client.close()

    _persist_env_value(env_path, "API_KEY_PRIVATE_KEY", private_key)
    _persist_env_value(env_path, "LIGHTER_API_KEY_INDEX", str(api_key_index))
    _persist_env_value(env_path, "LIGHTER_ACCOUNT_INDEX", str(account_index))

    logger.info("Lighter provisioning complete. Credentials persisted to %s", env_path)


class HedgingCycleExecutor:
    """Coordinates the four-leg hedging cycle between Aster and Lighter."""

    @staticmethod
    def _normalize_agent_identifier(value: Optional[str]) -> str:
        text = ""
        if value is not None:
            try:
                text = str(value).strip()
            except Exception:
                text = ""

        if not text:
            try:
                text = socket.gethostname()
            except Exception:
                text = ""
            else:
                text = (text or "").strip()

        if not text:
            text = "default"

        if len(text) > 120:
            text = text[:120]

        return text

    def __init__(self, config: CycleConfig):
        self.config = config
        self.config.enforce_min_cycle_interval = bool(
            getattr(config, "enforce_min_cycle_interval", True)
        )
        ticker_label = f"{config.aster_ticker}_{config.lighter_ticker}".replace("/", "-")
        self.logger = TradingLogger(
            exchange="hedge",
            ticker=ticker_label,
            log_to_console=bool(getattr(config, "log_to_console", False)),
        )

        agent_id_raw = getattr(config, "coordinator_agent_id", None)
        self._coordinator_agent_id = self._normalize_agent_identifier(agent_id_raw)
        self.config.coordinator_agent_id = self._coordinator_agent_id
        self.logger.log(
            f"Hedge coordinator agent id resolved to '{self._coordinator_agent_id}'",
            "INFO",
        )
        self._coordinator_username = (
            getattr(config, "coordinator_username", None) or ""
        ).strip() or None
        self._coordinator_password = (
            getattr(config, "coordinator_password", None) or ""
        ).strip() or None
        self._coordinator_paused = False
        self._pause_poll_seconds = max(1.0, float(getattr(config, "coordinator_pause_poll_seconds", 5.0) or 5.0))
        self._last_reported_position = Decimal("0")

        self._lighter_quantity_min = config.lighter_quantity_min
        self._lighter_quantity_max = config.lighter_quantity_max
        self._lighter_quantity_step = Decimal("0.001")
        self._current_cycle_lighter_quantity: Optional[Decimal] = None
        self._preserve_initial_lighter_position = bool(
            getattr(config, "preserve_initial_position", False)
        )
        self._baseline_lighter_position: Optional[Decimal] = None
        self._virtual_price_source = (config.virtual_aster_price_source or "aster").lower()
        allowed_virtual_sources = {"aster", "bn"}
        if self._virtual_price_source not in allowed_virtual_sources:
            self.logger.log(
                f"Invalid virtual maker price source '{config.virtual_aster_price_source}', fallback to 'aster'",
                "WARNING",
            )
            self._virtual_price_source = "aster"
        self._virtual_reference_symbol = config.virtual_aster_reference_symbol
        self._binance_price_client: Optional["_BinanceFuturesPriceSource"] = None
        self._metrics_reporter: Optional[HedgeMetricsReporter] = None

        base_depth_candidate = getattr(config, "aster_maker_depth_level", DEFAULT_ASTER_MAKER_DEPTH_LEVEL)
        self._aster_maker_depth_level = self._normalize_depth_value(
            base_depth_candidate,
            "aster_maker_depth_level",
            DEFAULT_ASTER_MAKER_DEPTH_LEVEL,
        )

        leg1_candidate = getattr(config, "aster_leg1_depth_level", None)
        leg3_candidate = getattr(config, "aster_leg3_depth_level", None)

        self._aster_leg1_depth_level = self._normalize_depth_value(
            leg1_candidate,
            "aster_leg1_depth_level",
            self._aster_maker_depth_level,
        )
        self._aster_leg3_depth_level = self._normalize_depth_value(
            leg3_candidate,
            "aster_leg3_depth_level",
            self._aster_maker_depth_level,
        )

        self.config.aster_maker_depth_level = self._aster_maker_depth_level
        self.config.aster_leg1_depth_level = self._aster_leg1_depth_level
        self.config.aster_leg3_depth_level = self._aster_leg3_depth_level
        self.logger.log(
            (
                "Using Aster depth levels -> leg1: "
                f"{self._aster_leg1_depth_level}, leg3: {self._aster_leg3_depth_level} "
                f"(default {self._aster_maker_depth_level})"
            ),
            "INFO",
        )

        self._randomize_direction = bool(getattr(config, "randomize_direction", False))
        seed_value = getattr(config, "direction_seed", None)
        self._direction_rng: Optional[random.Random] = None
        if self._randomize_direction:
            self._direction_rng = random.Random(seed_value)
        self._current_cycle_entry_direction: str = config.direction

        base_lighter_quantity = config.lighter_quantity
        if self._lighter_quantity_min is not None and self._lighter_quantity_max is not None:
            base_lighter_quantity = self._lighter_quantity_max

        self.aster_config = TradingConfig(
            ticker=config.aster_ticker.upper(),
            contract_id="",
            quantity=config.aster_quantity,
            take_profit=config.take_profit_pct,
            tick_size=Decimal(0),
            direction=config.direction,
            max_orders=1,
            wait_time=0,
            exchange="aster",
            grid_step=Decimal("-100"),
            stop_price=Decimal("-1"),
            pause_price=Decimal("-1"),
            boost_mode=False,
            maker_depth_level=self._aster_leg1_depth_level,
        )

        self.lighter_config = TradingConfig(
            ticker=config.lighter_ticker,
            contract_id="",
            quantity=base_lighter_quantity,
            take_profit=config.take_profit_pct,
            tick_size=Decimal(0),
            direction=self._lighter_initial_direction(),
            max_orders=1,
            wait_time=0,
            exchange="lighter",
            grid_step=Decimal("-100"),
            stop_price=Decimal("-1"),
            pause_price=Decimal("-1"),
            boost_mode=False,
        )

        self.aster_client: Optional["AsterClient"] = None
        self.lighter_client: Optional["LighterClient"] = None
        self._aster_public_data: Optional[_AsterPublicDataClient] = None
        self._last_leg1_price: Optional[Decimal] = None
        self._housekeeping_task: Optional[asyncio.Task] = None
        # Flag to indicate whether current cycle experienced a Lighter timeout
        self._cycle_had_timeout: bool = False
        # Tracemalloc state
        self._tracemalloc_started: bool = False
        self._last_tracemalloc_snapshot = None
        self._lighter_l1_address = (os.getenv("L1_WALLET_ADDRESS") or "").strip()
        self._lighter_intent_address: Optional[str] = None
        self._leaderboard_address_warning_emitted = False
        self._cached_leaderboard_points: Optional[Tuple[Optional[Decimal], Optional[Decimal]]] = None
        self._leaderboard_points_cycle: int = 0

    def _normalize_depth_value(self, raw_value: Optional[int], label: str, fallback: int) -> int:
        candidate = fallback if raw_value is None else raw_value
        try:
            depth = int(candidate)
        except (TypeError, ValueError):
            self.logger.log(
                f"Invalid {label} '{candidate}', defaulting to {fallback}",
                "WARNING",
            )
            depth = fallback

        if depth < 1:
            self.logger.log(
                f"{label} {depth} < 1; clamping to 1",
                "WARNING",
            )
            depth = 1
        elif depth > 500:
            self.logger.log(
                f"{label} {depth} > 500; clamping to 500",
                "WARNING",
            )
            depth = 500

        return depth

    async def _capture_initial_lighter_position(self) -> None:
        if not self.lighter_client:
            return

        try:
            position_raw = await self.lighter_client.get_account_positions()
        except Exception as exc:
            self.logger.log(
                f"Failed to capture initial Lighter position: {exc}",
                "WARNING",
            )
            return

        try:
            baseline = position_raw if isinstance(position_raw, Decimal) else Decimal(str(position_raw))
        except (InvalidOperation, ValueError, TypeError):
            self.logger.log(
                "Unable to interpret initial Lighter position; defaulting baseline to 0",
                "WARNING",
            )
            baseline = Decimal("0")

        self._baseline_lighter_position = baseline
        self.logger.log(
            (
                "Initial Lighter position preservation enabled; baseline captured at "
                f"{_format_decimal(baseline)} contracts"
            ),
            "INFO",
        )

    async def _refresh_pause_state(self) -> bool:
        if not self._metrics_reporter:
            self._coordinator_paused = False
            return False

        try:
            snapshot = await self._metrics_reporter.fetch_control(agent_id=self._coordinator_agent_id)
        except Exception as exc:
            self.logger.log(
                f"Unable to query coordinator control state: {exc}",
                "WARNING",
            )
            return self._coordinator_paused

        paused = False
        if isinstance(snapshot, dict):
            agent_state: Optional[Dict[str, Any]] = None
            candidate = snapshot.get("agent")
            if isinstance(candidate, dict):
                agent_state = candidate
            elif isinstance(snapshot.get("controls"), dict) and self._coordinator_agent_id:
                controls_dict = snapshot.get("controls")
                if isinstance(controls_dict, dict):
                    raw_agent_state = controls_dict.get(self._coordinator_agent_id)
                    if isinstance(raw_agent_state, dict):
                        agent_state = raw_agent_state

            if agent_state is not None:
                paused = bool(agent_state.get("paused", False))
            elif isinstance(snapshot.get("paused"), bool):
                paused = bool(snapshot.get("paused"))

        self._coordinator_paused = paused
        return paused

    async def wait_for_resume(self, context: str) -> None:
        if not self._metrics_reporter:
            self._coordinator_paused = False
            return

        delay = max(1.0, float(self._pause_poll_seconds))
        first_notification = True

        while True:
            paused = await self._refresh_pause_state()
            if not paused:
                if not first_notification:
                    self.logger.log(
                        f"Coordinator pause cleared; resuming ({context})",
                        "INFO",
                    )
                return

            if first_notification:
                self.logger.log(
                    f"Coordinator pause active ({context}); waiting for resume signal",
                    "WARNING",
                )
                first_notification = False

            await asyncio.sleep(delay)

    async def report_metrics(
        self,
        *,
        total_cycles: int,
        cumulative_pnl: Decimal,
        cumulative_volume: Decimal,
    ) -> None:
        if not self._metrics_reporter:
            return

        await self._refresh_pause_state()

        position = self._last_reported_position
        available_balance: Optional[Decimal] = None
        total_account_value: Optional[Decimal] = None
        instrument_label = f"{self.config.aster_ticker.upper()} / {self.config.lighter_ticker}".strip()
        depth_snapshot = {
            "maker": int(self._aster_maker_depth_level),
            "leg1": int(self._aster_leg1_depth_level),
            "leg3": int(self._aster_leg3_depth_level),
        }
        if self.lighter_client and not self._coordinator_paused:
            try:
                raw_position = await self.lighter_client.get_account_positions()
                if isinstance(raw_position, Decimal):
                    position = raw_position
                else:
                    position = Decimal(str(raw_position))
                self._last_reported_position = position
            except (InvalidOperation, ValueError, TypeError) as exc:
                self.logger.log(
                    f"Unexpected Lighter position payload for coordinator report: {exc}",
                    "WARNING",
                )
            except Exception as exc:
                self.logger.log(
                    f"Unable to fetch Lighter position for coordinator report: {exc}",
                    "WARNING",
                )
            try:
                account_snapshot = await self.lighter_client.get_account_metrics()
            except Exception as exc:
                self.logger.log(
                    f"Unable to fetch Lighter account metrics for coordinator report: {exc}",
                    "WARNING",
                )
            else:
                if isinstance(account_snapshot, dict):
                    available_candidate = account_snapshot.get("available_balance")
                    total_candidate = account_snapshot.get("total_account_value")
                    if total_candidate is None:
                        total_candidate = account_snapshot.get("total_asset_value")

                    try:
                        if available_candidate is not None:
                            available_balance = Decimal(str(available_candidate))
                    except (InvalidOperation, ValueError, TypeError):
                        available_balance = None
                        self.logger.log(
                            f"Invalid available balance in account metrics: {available_candidate}",
                            "WARNING",
                        )

                    try:
                        if total_candidate is not None:
                            total_account_value = Decimal(str(total_candidate))
                    except (InvalidOperation, ValueError, TypeError):
                        total_account_value = None
                        self.logger.log(
                            f"Invalid total account value in account metrics: {total_candidate}",
                            "WARNING",
                        )

        try:
            await self._metrics_reporter.report(
                position=position,
                total_cycles=total_cycles,
                cumulative_pnl=cumulative_pnl,
                cumulative_volume=cumulative_volume,
                agent_id=self._coordinator_agent_id,
                available_balance=available_balance,
                total_account_value=total_account_value,
                instrument=instrument_label,
                depths=depth_snapshot,
            )
        except Exception as exc:
            self.logger.log(
                f"Failed to push metrics to coordinator: {exc}",
                "WARNING",
            )

    def apply_depth_config(self, payload: Dict[str, Any]) -> None:
        if not payload:
            return

        base_candidate = payload.get("aster_maker_depth_level", self._aster_maker_depth_level)
        leg1_candidate = payload.get("aster_leg1_depth_level", base_candidate)
        leg3_candidate = payload.get("aster_leg3_depth_level", base_candidate)

        new_base = self._normalize_depth_value(base_candidate, "hot_aster_maker_depth_level", self._aster_maker_depth_level)
        new_leg1 = self._normalize_depth_value(leg1_candidate, "hot_aster_leg1_depth_level", new_base)
        new_leg3 = self._normalize_depth_value(leg3_candidate, "hot_aster_leg3_depth_level", new_base)

        if (
            new_base == self._aster_maker_depth_level
            and new_leg1 == self._aster_leg1_depth_level
            and new_leg3 == self._aster_leg3_depth_level
        ):
            return

        self._aster_maker_depth_level = new_base
        self._aster_leg1_depth_level = new_leg1
        self._aster_leg3_depth_level = new_leg3

        self.config.aster_maker_depth_level = new_base
        self.config.aster_leg1_depth_level = new_leg1
        self.config.aster_leg3_depth_level = new_leg3
        self.aster_config.maker_depth_level = new_leg1

        self.logger.log(
            (
                "Hot update applied -> "
                f"maker={new_base}, leg1={new_leg1}, leg3={new_leg3}"
            ),
            "INFO",
        )

    async def _memory_housekeeping_loop(self) -> None:
        """Periodically trigger GC and clean bounded in-memory states.

        Controlled by run arguments (CycleConfig):
        - memory_clean_interval_seconds: interval seconds (default 300)
        - memory_warn_mb: if >0, warn when rss exceeds threshold (best-effort)
        """
        interval_s = float(getattr(self.config, "memory_clean_interval_seconds", 300.0) or 300.0)
        warn_mb = float(getattr(self.config, "memory_warn_mb", 0.0) or 0.0)
        # Clamp interval to sensible bounds
        interval_s = max(30.0, min(interval_s, 3600.0))

        # Initialize tracemalloc if requested
        try:
            if getattr(self.config, "tracemalloc_enabled", False) and not tracemalloc.is_tracing():
                frames = int(getattr(self.config, "tracemalloc_frames", 25) or 25)
                frames = max(1, min(frames, 50))
                tracemalloc.start(frames)
                self._tracemalloc_started = True
                self.logger.log(f"Tracemalloc started with {frames} frames", "INFO")
        except Exception as exc:
            self.logger.log(f"Failed to start tracemalloc: {exc}", "WARNING")

        while True:
            try:
                await asyncio.sleep(interval_s)

                # Log current memory (best-effort)
                if warn_mb > 0:
                    try:
                        _psutil = __import__("psutil")
                        rss = _psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
                        if rss >= warn_mb:
                            self.logger.log(f"Process RSS {rss:.1f} MB exceeds threshold {warn_mb:.1f} MB", "WARNING")
                    except Exception:
                        pass

                # Force GC
                try:
                    counts_before = gc.get_count()
                    gc.collect()
                    counts_after = gc.get_count()
                    self.logger.log(
                        f"Housekeeping GC: gen_counts {counts_before} -> {counts_after}",
                        "INFO",
                    )
                except Exception:
                    pass

                # Ask Lighter WS to trim order book aggressively (already bounded, but proactive)
                try:
                    if self.lighter_client and getattr(self.lighter_client, "ws_manager", None):
                        ws = getattr(self.lighter_client, "ws_manager", None)
                        if ws and hasattr(ws, "cleanup_old_order_book_levels"):
                            ws.cleanup_old_order_book_levels()
                except Exception:
                    pass

                # Prune Lighter caches if large
                try:
                    if self.lighter_client and hasattr(self.lighter_client, "prune_caches"):
                        self.lighter_client.prune_caches()
                except Exception:
                    pass

                # Tracemalloc snapshot and top-N stats (best-effort)
                try:
                    if getattr(self.config, "tracemalloc_enabled", False) and tracemalloc.is_tracing():
                        group_by = (getattr(self.config, "tracemalloc_group_by", "lineno") or "lineno").lower()
                        top_n = int(getattr(self.config, "tracemalloc_top", 15) or 15)
                        top_n = max(5, min(top_n, 50))
                        filt_sub = getattr(self.config, "tracemalloc_filter", None)

                        snap = tracemalloc.take_snapshot()
                        stats = None
                        if self._last_tracemalloc_snapshot is not None:
                            stats = snap.compare_to(self._last_tracemalloc_snapshot, group_by)
                        else:
                            stats = snap.statistics(group_by)

                        # Optional filtering by substring in traceback or filename
                        if filt_sub:
                            fs = []
                            for st in stats:
                                try:
                                    tb = st.traceback
                                    if any((filt_sub in str(fr.filename)) for fr in tb):
                                        fs.append(st)
                                except Exception:
                                    continue
                            stats = fs

                        # Log top-N entries
                        self.logger.log("Tracemalloc top allocations:", "INFO")
                        for i, st in enumerate(stats[:top_n], start=1):
                            try:
                                size_mb = float(getattr(st, "size", 0)) / (1024 * 1024)
                                count = int(getattr(st, "count", 0)) if hasattr(st, "count") else 0

                                # Extract traceback frames (most-recent allocation frame first)
                                try:
                                    tb_frames = list(st.traceback)
                                except Exception:
                                    tb_frames = []

                                # Allocation site: closest frame to the actual allocation
                                alloc_fr = tb_frames[0] if tb_frames else None

                                # Business site: the first frame whose filename contains the filter substring (closest to allocation)
                                app_fr = None
                                if filt_sub and tb_frames:
                                    try:
                                        for fr in tb_frames:
                                            if filt_sub in str(fr.filename):
                                                app_fr = fr
                                                break
                                    except Exception:
                                        app_fr = None

                                # Fallback when no filter provided: try to pick a non-site-packages frame near the allocation
                                if app_fr is None and tb_frames:
                                    try:
                                        for fr in tb_frames:
                                            fname = str(fr.filename)
                                            if "site-packages" not in fname and "/env/" not in fname and "\\env\\" not in fname:
                                                app_fr = fr
                                                break
                                    except Exception:
                                        app_fr = None

                                # Compose locations
                                alloc_loc = f"{alloc_fr.filename}:{alloc_fr.lineno}" if alloc_fr else "<unknown>"
                                if app_fr and (not alloc_fr or (app_fr.filename != alloc_fr.filename or app_fr.lineno != alloc_fr.lineno)):
                                    app_loc = f"{app_fr.filename}:{app_fr.lineno}"
                                    msg = f"#{i:02d} {size_mb:.3f} MB in {count} blocks at {alloc_loc} | app {app_loc}"
                                else:
                                    msg = f"#{i:02d} {size_mb:.3f} MB in {count} blocks at {alloc_loc}"

                                self.logger.log(msg, "INFO")
                            except Exception:
                                continue

                        self._last_tracemalloc_snapshot = snap
                except Exception as exc:
                    self.logger.log(f"Tracemalloc sampling failed: {exc}", "WARNING")

            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Don't let housekeeping kill the executor
                self.logger.log(f"Housekeeping loop error: {exc}", "WARNING")
                continue

    def _lighter_initial_direction(self) -> str:
        return "sell" if self.config.direction == "buy" else "buy"

    async def _ensure_lighter_leverage(self, leverage: int) -> None:
        if leverage <= 0:
            return

        if not self.lighter_client:
            raise RuntimeError("Lighter client is not initialized")

        signer_client = getattr(self.lighter_client, "lighter_client", None)
        if signer_client is None:
            raise RuntimeError("Lighter signer client is not initialized")

        contract_id = getattr(self.lighter_client.config, "contract_id", None)
        if contract_id is None:
            raise RuntimeError("Lighter contract id not resolved yet")

        try:
            market_index = int(contract_id)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid Lighter market index: {contract_id}") from exc

        margin_mode = getattr(signer_client, "CROSS_MARGIN_MODE", None)
        if margin_mode is None:
            raise RuntimeError("Unable to determine Lighter margin mode for leverage update")

        self.logger.log(
            f"Updating Lighter leverage to {leverage}x (market {market_index})",
            "INFO",
        )

        tx_info, _, err = await signer_client.update_leverage(
            market_index,
            margin_mode,
            int(leverage),
        )

        if err is not None:
            message = str(err)
            lowered = message.lower()
            if "same" in lowered or "already" in lowered:
                self.logger.log(
                    f"Lighter leverage already set to {leverage}x; no change required",
                    "INFO",
                )
                return

            raise RuntimeError(f"Failed to update Lighter leverage to {leverage}x: {message}")

        self.logger.log(
            f"Lighter leverage updated to {leverage}x (tx={tx_info})",
            "INFO",
        )

    async def _resolve_lighter_l1_address(self) -> Optional[str]:
        if self._lighter_l1_address:
            return self._lighter_l1_address

        lighter_client = self.lighter_client
        base_url = LIGHTER_MAINNET_BASE_URL
        account_index: Optional[int] = None

        if lighter_client is not None:
            base_url = getattr(lighter_client, "base_url", base_url) or base_url
            account_index = getattr(lighter_client, "account_index", None)

        if account_index is None:
            raw_index = os.getenv("LIGHTER_ACCOUNT_INDEX")
            if raw_index:
                try:
                    account_index = int(raw_index)
                except (TypeError, ValueError):
                    account_index = None

        if account_index is None:
            return None

        api_client = getattr(lighter_client, "api_client", None)
        close_client = False
        if api_client is None:
            config = Configuration(host=base_url.rstrip("/"))
            api_client = ApiClient(configuration=config)
            close_client = True

        account_api = AccountApi(api_client=api_client)
        response: Optional[object] = None
        try:
            response = await account_api.account(by="index", value=str(account_index))
        except Exception as exc:
            self.logger.log(
                f"Failed to resolve Lighter L1 address for account index {account_index}: {exc}",
                "WARNING",
            )
            return None
        finally:
            if close_client and api_client is not None:
                try:
                    await api_client.close()
                except Exception:
                    pass

        accounts = getattr(response, "accounts", None) or []
        for account in accounts:
            l1_candidate = getattr(account, "l1_address", None) or getattr(account, "l1Address", None)
            if not l1_candidate:
                continue
            try:
                normalized = Web3.to_checksum_address(str(l1_candidate))
            except Exception:
                normalized = str(l1_candidate)

            self._lighter_l1_address = normalized
            os.environ["L1_WALLET_ADDRESS"] = normalized
            return self._lighter_l1_address

        return None

    async def ensure_l1_top_up_if_needed(self) -> None:
        if not self.lighter_client:
            return

        rpc_url = (
            os.getenv("ARBITRUM_RPC_URL")
            or os.getenv("L1_RPC_URL")
            or os.getenv("ARBITRUM_ONE_RPC_URL")
            or ""
        ).strip()

        if not rpc_url:
            self.logger.log(
                "ARBITRUM RPC endpoint not configured; skipping L1 USDC top-up check",
                "WARNING",
            )
            return

        private_key_env = (
            os.getenv("L1_WALLET_PRIVATE_KEY")
            or os.getenv("LIGHTER_L1_PRIVATE_KEY")
            or ""
        ).strip()

        address = await self._resolve_lighter_l1_address()
        if not address:
            address = (os.getenv("L1_WALLET_ADDRESS") or "").strip()

        normalized_private_key = _normalize_private_key_hex(private_key_env) if private_key_env else ""

        if not address and normalized_private_key:
            try:
                account = Account.from_key(normalized_private_key)
                address = Web3.to_checksum_address(account.address)
                self._lighter_l1_address = address
            except Exception as exc:
                self.logger.log(
                    f"Failed to derive L1 address from private key: {exc}",
                    "WARNING",
                )

        if not address:
            self.logger.log("Unable to resolve L1 wallet address; skipping top-up check", "WARNING")
            return

        web3 = Web3(Web3.HTTPProvider(rpc_url))
        if not web3.is_connected():
            raise SkipCycleError(f"Cannot reach Arbitrum RPC at {rpc_url}; deferring cycle")

        try:
            checksum_address = Web3.to_checksum_address(address)
        except Exception as exc:
            raise SkipCycleError(f"Invalid L1 wallet address '{address}': {exc}") from exc

        contract = _build_usdc_contract(web3)

        try:
            decimals_raw = contract.functions.decimals().call()
            decimals = int(decimals_raw)
            raw_balance = int(contract.functions.balanceOf(checksum_address).call())
        except Exception as exc:
            raise SkipCycleError(f"Failed to query L1 USDC balance: {exc}") from exc

        threshold_raw = 10 ** decimals
        if raw_balance <= threshold_raw:
            return

        if not normalized_private_key:
            raise SkipCycleError("L1 wallet private key missing; cannot top-up automatically")

        human_balance = _format_usdc_amount_raw(raw_balance, decimals)
        self.logger.log(
            f"Detected L1 USDC balance {human_balance} above {L1_USDC_TOPUP_THRESHOLD} USDC; initiating transfer",
            "INFO",
        )

        base_url = (
            getattr(self.lighter_client, "base_url", None)
            or os.getenv("LIGHTER_BASE_URL")
            or LIGHTER_MAINNET_BASE_URL
        ).strip()
        if not base_url:
            base_url = LIGHTER_MAINNET_BASE_URL

        intent_address = self._lighter_intent_address
        if not intent_address:
            try:
                intent_address = await _request_lighter_intent_address(base_url, checksum_address)
            except Exception as exc:
                raise SkipCycleError(f"Failed to obtain Lighter deposit address: {exc}") from exc
            self._lighter_intent_address = intent_address

        pre_balance: Optional[Decimal] = None
        try:
            pre_balance = await self.lighter_client.get_available_balance()
        except Exception as exc:
            self.logger.log(
                f"Unable to fetch Lighter available balance before top-up: {exc}",
                "WARNING",
            )

        tx_hash, transferred_raw, transfer_decimals = await _transfer_full_usdc_balance(
            web3,
            normalized_private_key,
            checksum_address,
            intent_address,
        )

        transfer_display = _format_usdc_amount_raw(transferred_raw, transfer_decimals)
        self.logger.log(
            f"Submitted L1→Lighter USDC transfer tx={tx_hash} amount={transfer_display}",
            "INFO",
        )

        expected_balance: Optional[Decimal] = None
        if pre_balance is not None:
            try:
                transfer_amount = Decimal(transferred_raw) / (Decimal(10) ** Decimal(transfer_decimals))
                expected_balance = pre_balance + transfer_amount
            except (InvalidOperation, ValueError):
                expected_balance = None

        wait_logger = logging.getLogger("lighter.topup")
        wait_logger.setLevel(logging.INFO)

        try:
            _, credited_balance = await _wait_for_lighter_account_ready(
                base_url,
                checksum_address,
                expected_balance=expected_balance,
                logger=wait_logger,
                timeout_seconds=600.0,
                poll_interval=30.0,
            )
        except Exception as exc:
            self.logger.log(
                f"Lighter balance confirmation after top-up incomplete: {exc}",
                "WARNING",
            )
        else:
            balance_display = f"{credited_balance.normalize():f}" if credited_balance else "0"
            self.logger.log(
                f"Lighter balance confirmed at {balance_display} USDC after top-up",
                "INFO",
            )

    async def _log_leaderboard_points(self, cycle_number: int) -> None:
        address = await self._resolve_lighter_l1_address()
        if not address:
            if not self._leaderboard_address_warning_emitted:
                self.logger.log(
                    "Lighter L1 address unavailable; skipping leaderboard lookup",
                    "INFO",
                )
                self._leaderboard_address_warning_emitted = True
            return
        self._leaderboard_address_warning_emitted = False

        refresh_needed = False
        cached_points = self._cached_leaderboard_points
        if cached_points is None:
            refresh_needed = True
        elif cycle_number - self._leaderboard_points_cycle >= _LEADERBOARD_REFRESH_CYCLES:
            refresh_needed = True

        if refresh_needed:
            auth_token: Optional[str] = None
            lighter_exchange = self.lighter_client
            if lighter_exchange is not None:
                signer_client = getattr(lighter_exchange, "lighter_client", None)
                if signer_client is not None and hasattr(signer_client, "create_auth_token_with_expiry"):
                    try:
                        generated_token, token_error = signer_client.create_auth_token_with_expiry()
                    except Exception as exc:
                        self.logger.log(
                            f"Failed to create Lighter auth token for leaderboard request: {exc}",
                            "WARNING",
                        )
                    else:
                        if token_error:
                            self.logger.log(
                                f"Lighter auth token generation returned error: {token_error}",
                                "WARNING",
                            )
                        else:
                            auth_token = (generated_token or "").strip() or None

            try:
                weekly_points, total_points = await _fetch_lighter_leaderboard_points(
                    address,
                    base_url=getattr(self.lighter_client, "base_url", None) if self.lighter_client else None,
                    auth_token=auth_token,
                )
            except Exception as exc:
                self.logger.log(
                    f"Failed to retrieve Lighter leaderboard points for {address}: {exc}",
                    "WARNING",
                )
            else:
                if weekly_points is None and total_points is None:
                    self.logger.log(
                        f"Lighter leaderboard points unavailable for {address}",
                        "INFO",
                    )
                else:
                    self._cached_leaderboard_points = (weekly_points, total_points)
                    self._leaderboard_points_cycle = cycle_number
                    cached_points = self._cached_leaderboard_points

        if cached_points is None:
            return

        _print_leaderboard_points(
            cycle_number,
            cached_points[0],
            cached_points[1],
            logger=self.logger,
            to_console=bool(getattr(self.config, "log_to_console", False)),
        )

    async def setup(self) -> None:
        """Instantiate exchange clients, connect, and hydrate contract metadata."""

        lighter_client = ExchangeFactory.create_exchange("lighter", self.lighter_config)  # type: ignore[arg-type]
        self.lighter_client = cast("LighterClient", lighter_client)
        await self.lighter_client.connect()

        if self.config.virtual_aster_maker:
            self.aster_client = None
            if self._aster_public_data is not None:
                try:
                    await self._aster_public_data.aclose()
                except Exception:
                    pass
            self._aster_public_data = _AsterPublicDataClient(
                ticker=self.config.aster_ticker,
                logger=self.logger,
            )
            aster_contract_id, aster_tick = await self._aster_public_data.initialize()
            self.logger.log(
                "Aster virtual maker enabled: using public market data without API keys",
                "INFO",
            )
        else:
            if self._aster_public_data is not None:
                try:
                    await self._aster_public_data.aclose()
                except Exception:
                    pass
                self._aster_public_data = None
            aster_client = ExchangeFactory.create_exchange("aster", self.aster_config)  # type: ignore[arg-type]
            self.aster_client = cast("AsterClient", aster_client)
            await self.aster_client.connect()
            aster_contract_id, aster_tick = await self.aster_client.get_contract_attributes()

        lighter_contract_id, lighter_tick = await self.lighter_client.get_contract_attributes()

        # Persist Lighter contract metadata for later emergency handling and price formatting
        self.lighter_config.contract_id = lighter_contract_id
        self.lighter_config.tick_size = lighter_tick
        lighter_client_config = getattr(self.lighter_client, "config", None)
        if lighter_client_config is not None:
            lighter_client_config.contract_id = lighter_contract_id
            lighter_client_config.tick_size = lighter_tick

        lighter_step = Decimal("0.001")
        base_amount_multiplier = getattr(self.lighter_client, "base_amount_multiplier", None)
        try:
            if base_amount_multiplier and base_amount_multiplier > 0:
                lighter_step = Decimal("1") / Decimal(base_amount_multiplier)
        except (InvalidOperation, ValueError, TypeError):
            lighter_step = Decimal("0.001")

        if lighter_step <= 0:
            lighter_step = Decimal("0.001")

        self._lighter_quantity_step = lighter_step
        quantized_lighter_qty = self._quantize_quantity(self.config.lighter_quantity, lighter_step)
        if quantized_lighter_qty is not None:
            self.config.lighter_quantity = quantized_lighter_qty

        if self._lighter_quantity_min is not None:
            quantized_min = self._quantize_quantity(self._lighter_quantity_min, lighter_step)
            if quantized_min is not None:
                self._lighter_quantity_min = quantized_min
        if self._lighter_quantity_max is not None:
            quantized_max = self._quantize_quantity(self._lighter_quantity_max, lighter_step)
            if quantized_max is not None:
                self._lighter_quantity_max = quantized_max

        self.aster_config.contract_id = aster_contract_id
        self.aster_config.tick_size = aster_tick

        self.logger.log(
            (
                f"Contracts resolved | Aster: id={aster_contract_id}, tick={_format_decimal(aster_tick, 8)}; "
                f"Lighter: id={lighter_contract_id}, tick={_format_decimal(lighter_tick, 8)}"
            ),
            "INFO",
        )

        coordinator_url = (self.config.coordinator_url or "").strip()
        if coordinator_url:
            try:
                self._metrics_reporter = HedgeMetricsReporter(
                    coordinator_url,
                    agent_id=self._coordinator_agent_id,
                    auth_username=self._coordinator_username,
                    auth_password=self._coordinator_password,
                )
                auth_label = "with credentials" if self._coordinator_username or self._coordinator_password else "without credentials"
                self.logger.log(
                    f"Hedge coordinator reporting enabled (endpoint={coordinator_url}, {auth_label})",
                    "INFO",
                )
            except Exception as exc:
                self._metrics_reporter = None
                self.logger.log(
                    f"Failed to initialise hedge coordinator reporter: {exc}",
                    "WARNING",
                )

        target_leverage = getattr(self.config, "lighter_leverage", DEFAULT_LIGHTER_LEVERAGE) or DEFAULT_LIGHTER_LEVERAGE
        try:
            target_leverage = int(target_leverage)
        except (TypeError, ValueError):
            target_leverage = DEFAULT_LIGHTER_LEVERAGE
        await self._ensure_lighter_leverage(target_leverage)

        if not self._virtual_reference_symbol:
            self._virtual_reference_symbol = aster_contract_id

        if self._virtual_reference_symbol:
            normalized_symbol = (
                self._virtual_reference_symbol.replace("-", "").replace("/", "").strip().upper()
            )
            if normalized_symbol:
                self._virtual_reference_symbol = normalized_symbol

        if self._virtual_price_source == "bn" and not self._virtual_reference_symbol:
            raise ValueError("Virtual maker price source 'bn' requires a reference symbol")

        if self._virtual_price_source == "bn" and self._virtual_reference_symbol:
            self._binance_price_client = _BinanceFuturesPriceSource(
                symbol=self._virtual_reference_symbol,
                logger=self.logger,
            )
            self.logger.log(
                f"Virtual maker price source set to Binance ({self._virtual_reference_symbol})",
                "INFO",
            )
        if not await self.lighter_client.wait_for_market_data(timeout=10):
            self.logger.log(
                "Lighter market data did not become ready within 10 seconds; proceeding with caution",
                "WARNING",
            )

        if self._lighter_quantity_min is not None and self._lighter_quantity_max is not None:
            lighter_quantity_display = f"{self._lighter_quantity_min}-{self._lighter_quantity_max} (step {self._lighter_quantity_step})"
        else:
            lighter_quantity_display = f"{self.config.lighter_quantity}"

        self.logger.log(
            f"Configured leg quantities -> Aster: {self.config.aster_quantity}, Lighter: {lighter_quantity_display}",
            "INFO",
        )
        if self._preserve_initial_lighter_position and self._baseline_lighter_position is None:
            await self._capture_initial_lighter_position()
        self.logger.log("Hedging cycle setup complete", "INFO")

        # Start housekeeping loop
        if self._housekeeping_task is None or self._housekeeping_task.done():
            self._housekeeping_task = asyncio.create_task(self._memory_housekeeping_loop())

    async def shutdown(self) -> None:
        """Ensure both exchange connections are released."""
        # Stop housekeeping loop first
        if self._housekeeping_task and not self._housekeeping_task.done():
            self._housekeeping_task.cancel()
            try:
                await self._housekeeping_task
            except asyncio.CancelledError:
                pass
        tasks = []
        if self.aster_client:
            tasks.append(self.aster_client.disconnect())
        if self.lighter_client:
            tasks.append(self.lighter_client.disconnect())
        if self._aster_public_data is not None:
            try:
                await self._aster_public_data.aclose()
            except Exception:
                pass
            self._aster_public_data = None
        # Close Binance client session if used
        if self._binance_price_client is not None:
            try:
                await self._binance_price_client.aclose()
            except Exception:
                pass
        if self._metrics_reporter is not None:
            try:
                await self._metrics_reporter.aclose()
            except Exception:
                pass
            self._metrics_reporter = None
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.log("Hedging cycle shutdown complete", "INFO")

    async def execute_cycle(self) -> List[LegResult]:
        """Run a single four-leg hedging cycle and return execution summaries."""
        if not self.lighter_client:
            raise RuntimeError("Lighter client is not initialized. Call setup() first.")
        if not self.config.virtual_aster_maker and not self.aster_client:
            raise RuntimeError("Aster client is not initialized. Call setup() first.")
        if self.config.virtual_aster_maker and not (self._aster_public_data or self.aster_client):
            raise RuntimeError("Aster market data helper is not initialized. Call setup() first.")

        results: List[LegResult] = []

        # Leg 1: Aster maker entry
        # Reset per-cycle flags
        self._cycle_had_timeout = False
        self._last_leg1_price = None
        self._current_cycle_lighter_quantity = None
        if self._randomize_direction and self._direction_rng is not None:
            entry_direction = self._direction_rng.choice(["buy", "sell"])
            self._current_cycle_entry_direction = entry_direction
            self.logger.log(
                f"Cycle entry direction selected: {entry_direction.upper()} (randomized mode)",
                "INFO",
            )
        else:
            entry_direction = self.config.direction
            self._current_cycle_entry_direction = entry_direction
        leg1 = await self._execute_aster_maker(leg_name="LEG1", direction=entry_direction)
        results.append(leg1)

        # Leg 2: Lighter opposite taker
        leg2_direction = "sell" if entry_direction == "buy" else "buy"
        leg2 = await self._execute_lighter_taker(
            leg_name="LEG2", direction=leg2_direction, reference_price=leg1.price
        )
        results.append(leg2)

        # Leg 3: Aster reverse maker to flatten
        reverse_direction = "sell" if entry_direction == "buy" else "buy"
        leg3 = await self._execute_aster_reverse_maker(
            leg_name="LEG3", direction=reverse_direction
        )
        results.append(leg3)

        # Leg 4: Lighter reverse taker to flatten
        leg4_direction = entry_direction
        leg4 = await self._execute_lighter_taker(
            leg_name="LEG4", direction=leg4_direction, reference_price=leg3.price
        )
        results.append(leg4)

        self._current_cycle_lighter_quantity = None

        self.logger.log("Hedging cycle completed successfully", "INFO")
        return results

    async def _execute_aster_maker(self, leg_name: str, direction: str) -> LegResult:
        if self.config.virtual_aster_maker:
            return await self._simulate_virtual_aster_leg(
                leg_name=leg_name,
                direction=direction,
                quantity=self.config.aster_quantity,
                depth_level=self._aster_leg1_depth_level,
            )

        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        overall_start = time.time()
        attempt = 0
        skip_retry_delay = False
        while True:
            attempt += 1
            if attempt > 1 and not skip_retry_delay:
                await asyncio.sleep(self.config.retry_delay_seconds)
            skip_retry_delay = False

            self.logger.log(
                f"{leg_name} | Placing Aster maker open: qty={self.config.aster_quantity}, side={direction}",
                "DEBUG",
            )
            order_result = await self.aster_client.place_open_order(
                self.aster_config.contract_id, self.config.aster_quantity, direction
            )
            if not order_result.order_id:
                raise RuntimeError(f"{leg_name} | Aster order returned without an order id")

            try:
                fill_info = await self._wait_for_aster_fill(str(order_result.order_id), leg_name)
                break
            except TimeoutError:
                self.logger.log(
                    f"{leg_name} | Attempt {attempt} timed out. Repricing and retrying...",
                    "WARNING",
                )
                if self.config.max_retries > 0 and attempt >= self.config.max_retries:
                    raise
                skip_retry_delay = True
                continue

        latency = time.time() - overall_start

        self.logger.log(
            f"{leg_name} | Aster maker filled {fill_info.filled_size} @ {fill_info.price}",
            "INFO",
        )

        self._last_leg1_price = fill_info.price

        return LegResult(
            name=leg_name,
            exchange="aster",
            side=direction,
            quantity=fill_info.filled_size,
            price=fill_info.price,
            order_id=fill_info.order_id,
            status=fill_info.status,
            latency_seconds=latency,
            requested_price=fill_info.price,
        )

    async def _execute_aster_reverse_maker(
        self, leg_name: str, direction: str
    ) -> LegResult:
        if self.config.virtual_aster_maker:
            return await self._simulate_virtual_aster_leg(
                leg_name=leg_name,
                direction=direction,
                quantity=self.config.aster_quantity,
                depth_level=self._aster_leg3_depth_level,
            )

        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        reverse_quantity = await self.aster_client.get_account_positions()
        if reverse_quantity <= 0:
            self.logger.log(
                f"{leg_name} | No open position detected on Aster; falling back to configured quantity {self.config.aster_quantity}",
                "WARNING",
            )
            reverse_quantity = self.config.aster_quantity
        else:
            self.logger.log(
                f"{leg_name} | Using current Aster position {reverse_quantity} as close quantity",
                "INFO",
            )

        overall_start = time.time()
        attempt = 0
        skip_retry_delay = False
        while True:
            attempt += 1
            if attempt > 1 and not skip_retry_delay:
                await asyncio.sleep(self.config.retry_delay_seconds)
            skip_retry_delay = False

            target_price = await self._calculate_aster_maker_price(
                direction,
                depth_level=self._aster_leg3_depth_level,
            )
            self.logger.log(
                f"{leg_name} | Placing Aster reverse close: qty={reverse_quantity}, side={direction}, limit={target_price}",
                "DEBUG",
            )
            order_result = await self.aster_client.place_close_order(
                self.aster_config.contract_id, reverse_quantity, target_price, direction
            )
            if not order_result.order_id:
                raise RuntimeError(f"{leg_name} | Aster close order returned without an order id")

            try:
                fill_info = await self._wait_for_aster_fill(str(order_result.order_id), leg_name)
                break
            except TimeoutError:
                self.logger.log(
                    f"{leg_name} | Attempt {attempt} timed out. Repricing and retrying...",
                    "WARNING",
                )
                if self.config.max_retries > 0 and attempt >= self.config.max_retries:
                    raise
                skip_retry_delay = True
                continue

        latency = time.time() - overall_start

        self.logger.log(
            f"{leg_name} | Aster reverse maker filled {fill_info.filled_size} @ {fill_info.price}",
            "INFO",
        )

        return LegResult(
            name=leg_name,
            exchange="aster",
            side=direction,
            quantity=fill_info.filled_size,
            price=fill_info.price,
            order_id=fill_info.order_id,
            status=fill_info.status,
            latency_seconds=latency,
            requested_price=fill_info.price,
        )

    async def _simulate_virtual_aster_leg(
        self,
        leg_name: str,
        direction: str,
        quantity: Decimal,
        *,
        depth_level: int,
    ) -> LegResult:
        if not self._has_aster_market_data() and not (
            self._virtual_price_source == "bn" and self._binance_price_client
        ):
            raise RuntimeError("Aster market data source is not available")

        overall_start = time.time()
        attempt = 0
        skip_retry_delay = False
        last_error: Optional[str] = None

        while True:
            attempt += 1
            if attempt > 1 and not skip_retry_delay:
                await asyncio.sleep(self.config.retry_delay_seconds)
            skip_retry_delay = False

            try:
                target_price = await self._calculate_virtual_maker_price(
                    direction,
                    depth_level=depth_level,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                last_error = str(exc)
                self.logger.log(
                    f"{leg_name} | Virtual pricing attempt {attempt} failed: {last_error}",
                    "ERROR",
                )
                if self.config.max_retries > 0 and attempt >= self.config.max_retries:
                    raise RuntimeError(
                        f"{leg_name} | Unable to determine virtual Aster price after {attempt} attempts: {last_error}"
                    )
                continue

            try:
                fill_price, fill_timestamp = await self._wait_for_virtual_aster_fill(
                    leg_name=leg_name,
                    direction=direction,
                    target_price=target_price,
                )
                break
            except TimeoutError:
                self.logger.log(
                    f"{leg_name} | Virtual Aster order at {target_price} timed out on attempt {attempt}. Retrying...",
                    "WARNING",
                )
                if self.config.max_retries > 0 and attempt >= self.config.max_retries:
                    raise
                skip_retry_delay = True
                continue

        latency = max(fill_timestamp - overall_start, 0.0)
        order_id = f"virtual-{leg_name}-{int(fill_timestamp * 1000)}"

        source_label = "binance" if self._virtual_price_source == "bn" else "aster"
        self.logger.log(
            f"{leg_name} | Virtual Aster maker filled {quantity} @ {fill_price} (target {target_price}, source={source_label})",
            "INFO",
        )

        self._last_leg1_price = fill_price

        return LegResult(
            name=leg_name,
            exchange="aster",
            side=direction,
            quantity=quantity,
            price=fill_price,
            order_id=order_id,
            status="VIRTUAL_FILLED",
            latency_seconds=latency,
            requested_price=target_price,
        )

    async def _wait_for_virtual_aster_fill(
        self,
        leg_name: str,
        direction: str,
        target_price: Decimal,
    ) -> Tuple[Decimal, float]:
        has_aster_data = self._has_aster_market_data()
        if not has_aster_data and not (
            self._virtual_price_source == "bn" and self._binance_price_client
        ):
            raise RuntimeError("Aster market data source is not available")
        if has_aster_data:
            # Ensure contract id is present for downstream logging/behaviour.
            self._ensure_aster_contract_id()

        deadline = time.time() + self.config.max_wait_seconds
        poll_interval = max(self.config.poll_interval, 0.05)

        while True:
            if self._virtual_price_source == "bn" and self._binance_price_client:
                best_bid, best_ask = await self._binance_price_client.fetch_bbo_prices()
            else:
                best_bid, best_ask = await self._fetch_aster_bbo_prices()
            now = time.time()

            if direction == "buy":
                if best_ask > 0 and best_ask <= target_price:
                    return best_ask, now
            else:
                if best_bid > 0 and best_bid >= target_price:
                    return best_bid, now

            if now >= deadline:
                break

            await asyncio.sleep(poll_interval)

        raise TimeoutError(f"{leg_name} | Virtual fill not reached for target {target_price}")

    async def _execute_lighter_taker(
        self, leg_name: str, direction: str, reference_price: Decimal
    ) -> LegResult:
        if not self.lighter_client:
            raise RuntimeError("Lighter client is not connected")

        if reference_price <= 0:
            raise RuntimeError(f"{leg_name} | Invalid reference price {reference_price} for Lighter taker order")

        if self.config.max_retries <= 0:
            max_attempts = 5
        else:
            max_attempts = min(5, self.config.max_retries)
        current_slippage = self.config.slippage_pct
        last_error: Optional[Exception] = None
        if self._current_cycle_lighter_quantity is None:
            order_quantity = self._select_lighter_order_quantity()
            self._current_cycle_lighter_quantity = order_quantity
            quantity_source = "randomized"
        else:
            order_quantity = self._current_cycle_lighter_quantity
            quantity_source = "reused"

        self.logger.log(
            f"{leg_name} | Using Lighter order quantity {order_quantity} ({quantity_source})",
            "DEBUG",
        )

        for attempt in range(1, max_attempts + 1):
            target_price = self._calculate_taker_price_from_reference(
                direction=direction,
                reference_price=reference_price,
                slippage_pct=current_slippage,
            )
            self.logger.log(
                f"{leg_name} | Lighter taker intent: side={direction}, qty={order_quantity}, ref={reference_price}, slip={current_slippage}%, limit={target_price}",
                "DEBUG",
            )

            start = time.time()

            order_result = await self.lighter_client.place_limit_order(
                self.lighter_config.contract_id,
                order_quantity,
                target_price,
                direction,
            )

            if not order_result.success:
                error_message = order_result.error_message or "Unknown error"
                lowered = error_message.lower()
                if (
                    "accidental price" in lowered
                    and current_slippage > Decimal("0")
                    and attempt < max_attempts
                ):
                    self.logger.log(
                        f"{leg_name} | Lighter flagged 'accidental price' at slip={current_slippage}%. Tightening and retrying...",
                        "WARNING",
                    )
                    current_slippage = max(
                        (current_slippage / Decimal("2")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP),
                        Decimal("0"),
                    )
                    await asyncio.sleep(self.config.retry_delay_seconds)
                    continue

                last_error = RuntimeError(error_message)
                self.logger.log(
                    f"{leg_name} | Attempt {attempt} failed to place Lighter order: {error_message} (side={direction}, qty={order_quantity}, limit={target_price})",
                    "ERROR",
                )
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"{leg_name} | Failed to place Lighter order after {attempt} attempts: {error_message}"
                    )
                await asyncio.sleep(self.config.retry_delay_seconds)
                continue

            if not order_result.order_id:
                raise RuntimeError(f"{leg_name} | Lighter order returned without a client order id")
            else:
                self.logger.log(
                    f"{leg_name} | Lighter order placed: client_id={order_result.order_id}, side={direction}, qty={order_quantity}, limit={target_price}",
                    "INFO",
                )

            try:
                fill_info = await self._wait_for_lighter_fill(
                    str(order_result.order_id),
                    leg_name,
                    expected_fill_size=order_quantity,
                    expected_side=direction,
                )
            except SkipCycleError:
                self.logger.log(
                    f"{leg_name} | Skipping cycle after Lighter timeout/position mismatch (client_id={order_result.order_id})",
                    "WARNING",
                )
                raise

            latency = time.time() - start

            self.logger.log(
                f"{leg_name} | Lighter taker filled {fill_info.filled_size} @ {fill_info.price}",
                "INFO",
            )

            return LegResult(
                name=leg_name,
                exchange="lighter",
                side=direction,
                quantity=fill_info.filled_size,
                price=fill_info.price,
                order_id=str(fill_info.order_id),
                status=fill_info.status,
                latency_seconds=latency,
                requested_price=target_price,
                reference_price=reference_price,
            )

        raise RuntimeError(
            f"{leg_name} | Failed to execute Lighter taker leg after {max_attempts} attempts: {last_error}"
        )

    async def _wait_for_aster_fill(self, order_id: str, leg_name: str) -> OrderInfo:
        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        deadline = time.time() + self.config.max_wait_seconds
        last_status = None

        while time.time() < deadline:
            order_info = await self.aster_client.get_order_info(order_id)
            if order_info:
                last_status = order_info.status
                if order_info.status == "FILLED":
                    return order_info
                if order_info.status in {"CANCELED", "REJECTED", "EXPIRED"}:
                    raise RuntimeError(f"{leg_name} | Aster order ended with status {order_info.status}")
            await asyncio.sleep(self.config.poll_interval)

        await self._attempt_cancel_aster(order_id, leg_name, last_status)

        # One last check in case the order just filled while we were canceling.
        final_info = await self.aster_client.get_order_info(order_id)
        if final_info and final_info.status == "FILLED":
            self.logger.log(
                f"{leg_name} | Aster order {order_id} filled while waiting for cancellation.",
                "INFO",
            )
            return final_info

        raise TimeoutError(f"{leg_name} | Aster order {order_id} not filled within timeout")

    async def _wait_for_lighter_fill(
        self,
        client_order_id: str,
        leg_name: str,
        *,
        expected_final_position: Optional[Decimal] = None,
        expected_fill_size: Optional[Decimal] = None,
        expected_side: Optional[str] = None,
        position_before: Optional[Decimal] = None,
    ) -> OrderInfo:
        if not self.lighter_client:
            raise RuntimeError("Lighter client is not connected")

        deadline = time.time() + self.config.lighter_max_wait_seconds
        target_client_id = int(client_order_id)

        while time.time() < deadline:
            current_order = getattr(self.lighter_client, "current_order", None)
            client_identifier = getattr(self.lighter_client, "current_order_client_id", None)
            if current_order and client_identifier == target_client_id:
                status = current_order.status
                if status == "FILLED":
                    return current_order
                if status in {"CANCELED", "REJECTED", "EXPIRED"}:
                    raise RuntimeError(f"{leg_name} | Lighter order ended with status {status}")
            await asyncio.sleep(self.config.poll_interval)

        self.logger.log(
            (
                f"{leg_name} | Timeout reached waiting for Lighter (client_id={client_order_id}); "
                f"checking position with expectations: final={expected_final_position}, "
                f"side={expected_side}, size={expected_fill_size}"
            ),
            "WARNING",
        )
        # Mark that this cycle had a Lighter timeout to avoid skewed summary/PNL later
        try:
            self._cycle_had_timeout = True
        except Exception:
            pass

        position_after: Optional[Decimal] = None
        try:
            position_after = await self.lighter_client.get_account_positions()
        except Exception as exc:
            self.logger.log(
                f"{leg_name} | Failed to fetch Lighter position after timeout: {exc}",
                "ERROR",
            )

        current_order = getattr(self.lighter_client, "current_order", None)
        assumed_price = Decimal("0")
        if current_order is not None:
            price_candidate = getattr(current_order, "price", None)
            if isinstance(price_candidate, Decimal):
                assumed_price = price_candidate
            elif price_candidate is not None:
                try:
                    assumed_price = Decimal(str(price_candidate))
                except (InvalidOperation, ValueError, TypeError):
                    assumed_price = Decimal("0")

        expected_match = False
        tolerance = Decimal("0.0001")
        if expected_fill_size is not None and expected_fill_size > 0:
            tolerance = max(tolerance, expected_fill_size * Decimal("0.0001"))

        if expected_final_position is not None and position_after is not None:
            # Absolute target check: compare actual final position to expected final position
            expected_match = abs(position_after - expected_final_position) <= tolerance
        elif (
            expected_final_position is None
            and expected_fill_size is not None
            and expected_side is not None
            and position_after is not None
        ):
            # Delta-based check: if we don't know the starting position, assume we expect at least
            # expected_fill_size movement in the expected direction.
            side_norm = expected_side.lower()
            if side_norm == "buy":
                expected_match = position_after >= (expected_fill_size - tolerance)
            elif side_norm == "sell":
                expected_match = (-position_after) >= (expected_fill_size - tolerance)

        if expected_match:
            fill_size = expected_fill_size
            if fill_size is None and position_after is not None and position_before is not None:
                fill_size = abs(position_after - position_before)

            if fill_size is None:
                fill_size = Decimal("0")

            fill_size = abs(fill_size)
            side = (expected_side or getattr(current_order, "side", "buy")).lower()
            side = "buy" if side not in {"buy", "sell"} else side

            self.logger.log(
                (
                    f"{leg_name} | Lighter order {client_order_id} timed out but position "
                    f"matches expectation (pos={position_after}); treating as filled: side={side}, size={fill_size}, price={assumed_price}"
                ),
                "WARNING",
            )

            return OrderInfo(
                order_id=client_order_id,
                side=side,
                size=Decimal(fill_size),
                price=assumed_price,
                status="FILLED",
                filled_size=Decimal(fill_size),
                remaining_size=Decimal("0"),
            )

        message = (
            f"{leg_name} | Lighter order {client_order_id} timed out; "
            f"position={position_after} did not meet expected_final={expected_final_position} "
            f"(expected_side={expected_side}, expected_size={expected_fill_size}). Skipping cycle."
        )
        self.logger.log(message, "WARNING")
        raise SkipCycleError(message)

    async def _attempt_cancel_aster(
        self, order_id: str, leg_name: str, last_status: str | None
    ) -> None:
        if not self.aster_client:
            return

        try:
            result = await self.aster_client.cancel_order(order_id)
            if not result.success:
                message = (result.error_message or "").lower()
                if "unknown order" in message or "-2011" in message:
                    self.logger.log(
                        f"{leg_name} | Aster reports order {order_id} already closed ({result.error_message})",
                        "INFO",
                    )
                else:
                    self.logger.log(
                        f"{leg_name} | Failed to cancel Aster order {order_id}: {result.error_message}",
                        "ERROR",
                    )
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.log(
                f"{leg_name} | Exception canceling Aster order {order_id}: {exc} (last status={last_status})",
                "ERROR",
            )

    def _ensure_aster_contract_id(self) -> str:
        contract_id = self.aster_config.contract_id
        if not contract_id:
            raise RuntimeError("Aster contract id is not initialized")
        return contract_id

    def _has_aster_market_data(self) -> bool:
        return bool(self.aster_client or self._aster_public_data)

    async def _get_aster_depth_level_price(
        self, direction: str, level: int
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        if self._aster_public_data is not None:
            return await self._aster_public_data.get_depth_level_price(direction, level)
        if self.aster_client is not None:
            contract_id = self._ensure_aster_contract_id()
            return await self.aster_client.get_depth_level_price(contract_id, direction, level)
        raise RuntimeError("Aster market data source is unavailable")

    async def _fetch_aster_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        if self._aster_public_data is not None:
            return await self._aster_public_data.fetch_bbo_prices()
        if self.aster_client is not None:
            contract_id = self._ensure_aster_contract_id()
            return await self.aster_client.fetch_bbo_prices(contract_id)
        raise RuntimeError("Aster market data source is unavailable")

    async def _calculate_aster_maker_price(self, direction: str, *, depth_level: Optional[int] = None) -> Decimal:
        if not self._has_aster_market_data():
            raise RuntimeError("Aster market data source is not available")

        tick = self.aster_config.tick_size if self.aster_config.tick_size > 0 else Decimal("0")
        level = depth_level if depth_level is not None else self._aster_leg3_depth_level

        depth_price, best_bid, best_ask = await self._get_aster_depth_level_price(direction, level)

        best_bid = best_bid if best_bid is not None else Decimal("0")
        best_ask = best_ask if best_ask is not None else Decimal("0")

        # If no usable depth price, fetch fallback BBO
        if depth_price is None or depth_price <= 0:
            fb_bid, fb_ask = await self._fetch_aster_bbo_prices()
            best_bid = fb_bid if fb_bid > 0 else best_bid
            best_ask = fb_ask if fb_ask > 0 else best_ask

        price = self._compute_maker_price_from_data(
            source_label="ASTER",
            direction=direction,
            tick=tick,
            level=level,
            depth_price=depth_price,
            best_bid=best_bid,
            best_ask=best_ask,
        )
        return price

    async def _calculate_binance_maker_price(self, direction: str) -> Decimal:
        if not self._binance_price_client:
            raise RuntimeError("Binance price source is not configured")

        tick = self.aster_config.tick_size if self.aster_config.tick_size > 0 else Decimal("0")
        level = 4
        depth_price, best_bid, best_ask = await self._binance_price_client.get_depth_level_price(
            direction,
            level=level,
        )

        best_bid = best_bid if best_bid is not None else Decimal("0")
        best_ask = best_ask if best_ask is not None else Decimal("0")

        if depth_price is None or depth_price <= 0:
            fb_bid, fb_ask = await self._binance_price_client.fetch_bbo_prices()
            best_bid = fb_bid if fb_bid > 0 else best_bid
            best_ask = fb_ask if fb_ask > 0 else best_ask

        price = self._compute_maker_price_from_data(
            source_label="BINANCE",
            direction=direction,
            tick=tick,
            level=level,
            depth_price=depth_price,
            best_bid=best_bid,
            best_ask=best_ask,
        )
        return price

    def _compute_maker_price_from_data(
        self,
        *,
        source_label: str,
        direction: str,
        tick: Decimal,
        level: int,
        depth_price: Optional[Decimal],
        best_bid: Decimal,
        best_ask: Decimal,
    ) -> Decimal:
        price = depth_price if (depth_price is not None and depth_price > 0) else None

        if price is None:
            if direction == "sell":
                if best_ask > 0:
                    price = best_ask
                elif best_bid > 0 and tick > 0:
                    price = best_bid + tick
            else:
                if best_bid > 0:
                    price = best_bid
                elif best_ask > 0 and tick > 0:
                    price = best_ask - tick

        if price is None or price <= 0:
            raise ValueError(f"Unable to determine {source_label} maker price from market data")

        if tick > 0:
            price = _round_to_tick(price, tick)

        minimal_step = tick if tick > 0 else Decimal("0.00000001")

        if direction == "sell" and best_bid > 0 and price <= best_bid:
            price = best_bid + minimal_step
            if tick > 0:
                price = _round_to_tick(price, tick)
        elif direction == "buy" and best_ask > 0 and price >= best_ask:
            price = best_ask - minimal_step
            if tick > 0:
                price = _round_to_tick(price, tick)

        minimal_price = tick if tick > 0 else minimal_step
        if price < minimal_price:
            price = minimal_price
            if tick > 0:
                price = _round_to_tick(price, tick)

        if price <= 0:
            raise ValueError("Calculated maker price is non-positive")

        self.logger.log(
            (
                f"{source_label} maker price calc: dir={direction}, depth@{level}={depth_price}, "
                f"bbo=({_format_decimal(best_bid,8)},{_format_decimal(best_ask,8)}), tick={_format_decimal(tick,8)}, chosen={price}"
            ),
            "DEBUG",
        )
        return price

    async def _calculate_virtual_maker_price(
        self,
        direction: str,
        *,
        depth_level: int,
    ) -> Decimal:
        if self._virtual_price_source == "bn" and self._binance_price_client:
            return await self._calculate_binance_maker_price(direction)
        return await self._calculate_aster_maker_price(
            direction,
            depth_level=depth_level,
        )

    def _calculate_taker_price_from_reference(
        self,
        direction: str,
        reference_price: Decimal,
        slippage_pct: Decimal,
    ) -> Decimal:
        tick = self.lighter_config.tick_size if self.lighter_config.tick_size > 0 else Decimal("0")

        slip_fraction = max(slippage_pct, Decimal("0")) / Decimal("100")
        offset = reference_price * slip_fraction

        if tick > 0:
            offset = max(offset, tick)
        elif offset == 0:
            # Ensure a minimal offset to cross the spread when no tick is defined.
            offset = max(reference_price * Decimal("0.0001"), Decimal("1E-6"))

        if direction == "buy":
            price = reference_price + offset
        else:
            price = reference_price - offset
            if price <= 0:
                price = max(reference_price / Decimal("2"), tick if tick > 0 else Decimal("1E-6"))

        result = _round_to_tick(price, tick)
        self.logger.log(
            f"Lighter taker price calc: dir={direction}, ref={reference_price}, slip%={slippage_pct}, tick={_format_decimal(tick,8)}, result={result}",
            "DEBUG",
        )
        return result

    def _select_lighter_order_quantity(self) -> Decimal:
        if self._lighter_quantity_min is None or self._lighter_quantity_max is None:
            return self.config.lighter_quantity

        min_units = int(
            (self._lighter_quantity_min / self._lighter_quantity_step).to_integral_value(
                rounding=ROUND_HALF_UP
            )
        )
        max_units = int(
            (self._lighter_quantity_max / self._lighter_quantity_step).to_integral_value(
                rounding=ROUND_HALF_UP
            )
        )

        if max_units < min_units:
            raise ValueError("Configured Lighter quantity range is invalid")

        selected_units = random.randint(min_units, max_units)
        quantity = Decimal(selected_units) * self._lighter_quantity_step
        return quantity.quantize(self._lighter_quantity_step)

    @staticmethod
    def _quantize_quantity(quantity: Optional[Decimal], step: Decimal) -> Optional[Decimal]:
        if quantity is None or step <= 0:
            return quantity

        try:
            units = (quantity / step).to_integral_value(rounding=ROUND_HALF_UP)
        except (InvalidOperation, ZeroDivisionError):
            return quantity

        quantized = units * step
        try:
            return quantized.quantize(step)
        except (InvalidOperation, ValueError):
            return quantized

    def _calculate_emergency_limit_price(self, base_price: Decimal, side: str) -> Decimal:
        if base_price <= 0:
            raise ValueError("Base price for emergency order must be positive")

        tick = self.lighter_config.tick_size if self.lighter_config.tick_size > 0 else Decimal("0")
        slip_fraction = max(self.config.slippage_pct, Decimal("0")) / Decimal("100")
        if slip_fraction <= 0:
            slip_fraction = Decimal("0.0005")

        if side.lower() == "sell":
            price = base_price * (Decimal("1") - slip_fraction)
        else:
            price = base_price * (Decimal("1") + slip_fraction)

        if tick > 0:
            price = _round_to_tick(price, tick)

        minimal_step = tick if tick > 0 else Decimal("0.00000001")
        if price <= 0:
            price = minimal_step

        return price

    async def ensure_lighter_flat(self) -> None:
        if not self.lighter_client:
            self.logger.log("Lighter client unavailable; skipping emergency flatten", "WARNING")
            return

        try:
            current_position_raw = await self.lighter_client.get_account_positions()
        except Exception as exc:
            self.logger.log(
                f"Failed to query Lighter position during preservation check: {exc}",
                "ERROR",
            )
            return

        try:
            position = (
                current_position_raw
                if isinstance(current_position_raw, Decimal)
                else Decimal(str(current_position_raw))
            )
        except (InvalidOperation, ValueError, TypeError):
            self.logger.log(
                f"Unable to interpret Lighter position value '{current_position_raw}'; assuming 0",
                "WARNING",
            )
            position = Decimal("0")

        target_position = Decimal("0")
        if self._preserve_initial_lighter_position:
            if self._baseline_lighter_position is None:
                await self._capture_initial_lighter_position()
            if self._baseline_lighter_position is not None:
                target_position = self._baseline_lighter_position

        delta = position - target_position

        if delta == Decimal("0"):
            if self._preserve_initial_lighter_position:
                self.logger.log(
                    (
                        "Lighter position matches baseline "
                        f"{_format_decimal(target_position)}; no restoration required"
                    ),
                    "INFO",
                )
            else:
                self.logger.log("No Lighter position detected; no emergency action required", "INFO")
            if bool(getattr(self.config, "log_to_console", False)):
                print()
            return

        # Positive deltas denote excess long exposure relative to the target; negative deltas denote excess short exposure.
        side = "sell" if delta > 0 else "buy"
        quantity = abs(delta)

        if self._preserve_initial_lighter_position:
            self.logger.log(
                (
                    "Restoring Lighter position to baseline: current="
                    f"{_format_decimal(position)} contracts, target={_format_decimal(target_position)}, "
                    f"action={side.upper()} {_format_decimal(quantity)}"
                ),
                "INFO",
            )

        reference_price = self._last_leg1_price
        if reference_price is None or reference_price <= 0:
            try:
                best_bid, best_ask = await self.lighter_client.fetch_bbo_prices(self.lighter_config.contract_id)
                reference_price = best_bid if side == "sell" else best_ask
            except Exception as exc:  # pragma: no cover - fallback logging
                self.logger.log(
                    f"Unable to obtain reference price for emergency flatten: {exc}",
                    "ERROR",
                )
                return

        try:
            limit_price = self._calculate_emergency_limit_price(reference_price, side)
        except Exception as exc:
            self.logger.log(
                f"Failed to calculate emergency price using reference {reference_price}: {exc}",
                "ERROR",
            )
            return

        contract_id_value = self.lighter_config.contract_id
        if isinstance(contract_id_value, (int, Decimal)):
            contract_id_value = str(contract_id_value)
            self.lighter_config.contract_id = contract_id_value
        elif contract_id_value is not None:
            normalized_contract = str(contract_id_value).strip()
            if normalized_contract:
                self.lighter_config.contract_id = normalized_contract
                contract_id_value = normalized_contract
            else:
                contract_id_value = None

        fallback_client_config = getattr(self.lighter_client, "config", None)
        if contract_id_value in (None, "") and fallback_client_config is not None:
            fallback_contract = getattr(fallback_client_config, "contract_id", None)
            if fallback_contract not in (None, ""):
                self.lighter_config.contract_id = str(fallback_contract)
                contract_id_value = self.lighter_config.contract_id
            if self.lighter_config.tick_size <= 0:
                tick_candidate = getattr(fallback_client_config, "tick_size", None)
                if isinstance(tick_candidate, Decimal) and tick_candidate > 0:
                    self.lighter_config.tick_size = tick_candidate

        if contract_id_value in (None, ""):
            try:
                contract_id_raw, tick_size = await self.lighter_client.get_contract_attributes()
            except Exception as exc:
                self.logger.log(
                    f"Unable to recover Lighter contract metadata before emergency flatten: {exc}",
                    "ERROR",
                )
                return

            contract_id_value = str(contract_id_raw)
            self.lighter_config.contract_id = contract_id_value
            if isinstance(tick_size, Decimal) and tick_size > 0:
                self.lighter_config.tick_size = tick_size

            if fallback_client_config is not None:
                setattr(fallback_client_config, "contract_id", self.lighter_config.contract_id)
                setattr(fallback_client_config, "tick_size", self.lighter_config.tick_size)

        if contract_id_value in (None, ""):
            self.logger.log("Lighter contract ID missing; cannot place emergency order", "ERROR")
            return

        self.logger.log(
            f"Emergency flatten on Lighter: position={position}, side={side}, quantity={quantity}, limit={limit_price}",
            "WARNING",
        )

        order_result = await self.lighter_client.place_limit_order(
            self.lighter_config.contract_id,
            quantity,
            limit_price,
            side,
        )

        if not order_result.success or not order_result.order_id:
            self.logger.log(
                f"Emergency flatten order failed: {order_result.error_message}",
                "ERROR",
            )
            return

        try:
            fill_info = await self._wait_for_lighter_fill(
                str(order_result.order_id),
                "EMERGENCY_FLATTEN",
                expected_final_position=target_position,
                expected_fill_size=quantity,
                expected_side=side,
                position_before=position,
            )
            self.logger.log(
                f"Emergency flatten filled {fill_info.filled_size} @ {fill_info.price}",
                "INFO",
            )
        except Exception as exc:
            self.logger.log(
                f"Emergency flatten order {order_result.order_id} did not complete: {exc}",
                "ERROR",
            )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute a single Aster–Lighter hedging cycle using existing exchange adapters",
    )
    parser.add_argument("--aster-ticker", required=True, help="Symbol on Aster (e.g., ETH)")
    parser.add_argument(
        "--lighter-ticker",
        required=True,
        help="Symbol on Lighter (e.g., ETH-PERP, case-sensitive per exchange metadata)",
    )
    parser.add_argument(
        "--quantity",
        required=True,
        type=_decimal_type,
        help="Position size (contracts) for each leg",
    )
    parser.add_argument(
        "--aster-quantity",
        type=_decimal_type,
        help="Override quantity for Aster maker legs (defaults to --quantity)",
    )
    parser.add_argument(
        "--lighter-quantity",
        type=_decimal_type,
        help="Override quantity for Lighter taker legs (defaults to --quantity)",
    )
    parser.add_argument(
        "--lighter-quantity-min",
        type=_decimal_type,
        help="Optional minimum quantity for Lighter taker legs when randomizing",
    )
    parser.add_argument(
        "--lighter-quantity-max",
        type=_decimal_type,
        help="Optional maximum quantity for Lighter taker legs when randomizing",
    )
    parser.add_argument(
        "--direction",
        choices=["buy", "sell"],
        default="buy",
        help="Initial maker direction on Aster",
    )
    parser.add_argument(
        "--randomize-direction",
        action="store_true",
        help="Randomize the entry direction every cycle instead of using the fixed --direction",
    )
    parser.add_argument(
        "--direction-seed",
        type=int,
        help="Optional seed for random direction selection to make runs reproducible",
    )
    parser.add_argument(
        "--take-profit",
        type=_decimal_type,
        default=Decimal("0"),
        help="Reserved for compatibility (currently unused in reverse Aster leg)",
    )
    parser.add_argument(
        "--slippage",
        type=_decimal_type,
        default=Decimal("0.05"),
        help="Additional percentage applied to Lighter taker prices to ensure fills",
    )
    parser.add_argument(
        "--max-wait",
        type=float,
        default=5.0,
        help="Maximum seconds to wait for each leg to fill before canceling",
    )
    parser.add_argument(
        "--lighter-max-wait",
        type=float,
        default=120.0,
        help="Maximum seconds to wait for Lighter fills before canceling",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.1,
        help="Polling interval when waiting for order status updates",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=500,
        help="Maximum number of retries for Aster maker orders before aborting (set 0 for unlimited)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=5.0,
        help="Delay in seconds before retrying an Aster maker order",
    )
    parser.add_argument(
        "--aster-maker-depth",
        type=int,
        default=DEFAULT_ASTER_MAKER_DEPTH_LEVEL,
        help="Order book depth level used for Aster maker pricing (1-500)",
    )
    parser.add_argument(
        "--aster-leg1-depth",
        type=int,
        help="Override depth level for Aster leg1 maker entry (1-500). Defaults to --aster-maker-depth.",
    )
    parser.add_argument(
        "--aster-leg3-depth",
        type=int,
        help="Override depth level for Aster leg3 reverse maker (1-500). Defaults to --aster-maker-depth.",
    )
    parser.add_argument(
        "--virtual-aster-maker",
        action="store_true",
        help="Simulate Aster maker legs without sending real orders",
    )
    parser.add_argument(
        "--virtual-maker-price-source",
        choices=["aster", "bn"],
        default="aster",
        help="When --virtual-aster-maker is set, choose the market data source for virtual maker fills",
    )
    parser.add_argument(
        "--virtual-maker-symbol",
        help="Optional contract symbol used by the virtual maker price source (defaults to the resolved Aster contract id)",
    )
    parser.add_argument(
        "--preserve-initial-position",
        action="store_true",
        help=(
            "Capture the initial Lighter position at startup and restore it after each cycle and on shutdown "
            "instead of forcing the account flat"
        ),
    )
    parser.add_argument(
        "--hot-update-url",
        "--hot-depth-url",
        dest="hot_update_url",
        help=(
            "Optional HTTP(S) URL returning JSON overrides for hedging configuration (depth levels, cycle switch, etc.). "
            "Fetched with curl before each cycle"
        ),
    )
    parser.add_argument(
        "--coordinator-url",
        help="Optional base URL of the hedge coordinator/dashboard (e.g. http://localhost:8899)",
    )
    parser.add_argument(
        "--coordinator-agent",
        help="Identifier reported to the coordinator to distinguish this VPS (defaults to hostname)",
    )
    parser.add_argument(
        "--coordinator-username",
        help="Optional username for coordinator HTTP Basic auth",
    )
    parser.add_argument(
        "--coordinator-password",
        help="Optional password for coordinator HTTP Basic auth",
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Number of hedging cycles to run (0 means run continuously until interrupted)",
    )
    parser.add_argument(
        "--cycle-delay",
        type=float,
        default=0.0,
        help=(
            "Additional delay in seconds between successive hedging cycles. "
            "Actual cadence enforces at least 60 seconds between cycle starts unless --disable-min-cycle-interval is used"
        ),
    )
    parser.add_argument(
        "--disable-min-cycle-interval",
        action="store_true",
        help="Allow successive hedging cycles without enforcing the default 60-second minimum interval",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the environment file containing both exchange credentials",
    )
    parser.add_argument(
        "--lighter-leverage",
        type=int,
        default=DEFAULT_LIGHTER_LEVERAGE,
        help="Target leverage to enforce on Lighter before each run (1-125)",
    )
    parser.add_argument(
        "--l1-private-key",
        help=(
            "Optional L1 wallet private key. When provided, existing private key and Lighter API credentials "
            "are cleared, the new key is saved, and initialization continues"
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Console logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--log-to-console",
        dest="log_to_console",
        action="store_true",
        default=None,
        help="Also log strategy messages to stdout/stderr (default on; use --no-log-to-console to disable)",
    )
    parser.add_argument(
        "--no-log-to-console",
        dest="log_to_console",
        action="store_false",
        help="Disable console logging for the strategy output",
    )
    parser.add_argument(
        "--memory-clean-interval",
        type=float,
        default=300.0,
        help="Interval in seconds for periodic memory housekeeping (GC and cache trims). Clamp 30..3600; set 0 to use default",
    )
    parser.add_argument(
        "--memory-warn-mb",
        type=float,
        default=0.0,
        help="If > 0, emit a WARNING log when process RSS exceeds this threshold in MB (0 disables)",
    )
    parser.add_argument(
        "--tracemalloc",
        action="store_true",
        help="Enable periodic tracemalloc snapshots in housekeeping to find allocation hotspots",
    )
    parser.add_argument(
        "--tracemalloc-top",
        type=int,
        default=15,
        help="Number of top allocation entries to log per snapshot (5..50)",
    )
    parser.add_argument(
        "--tracemalloc-group-by",
        choices=["lineno", "traceback", "filename"],
        default="lineno",
        help="Group statistics by this key for tracemalloc reporting",
    )
    parser.add_argument(
        "--tracemalloc-filter",
        help="Only include allocations whose traceback contains this substring (e.g., 'perp-dex-tools')",
    )
    parser.add_argument(
        "--tracemalloc-frames",
        type=int,
        default=25,
        help="Number of frames to capture per allocation (1..50)",
    )
    parser.add_argument(
        "--reset-credentials",
        action="store_true",
        help="Remove L1 wallet private key and Lighter API credentials from the env file, then exit",
    )
    return parser.parse_args()


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def _print_summary(
    results: List[LegResult],
    cycle_number: Optional[int] = None,
    logger: Optional[TradingLogger] = None,
    *,
    to_console: bool = True,
) -> None:
    title = f"Cycle {cycle_number} Summary" if cycle_number is not None else "Cycle Summary"
    header = f"\n{title}"
    if to_console:
        print(header)
        print("=" * len(header))
    if logger:
        # Log only the header to the file to avoid duplicating each leg line
        logger.log(title, "INFO")
    for leg in results:
        parts = [
            f"{leg.name} | {leg.exchange.upper():7s} | {leg.side.upper():4s}",
            f"qty={_format_decimal(leg.quantity)}",
            f"fill={_format_decimal(leg.price, places=2)}",
            f"status={leg.status}",
            f"latency={leg.latency_seconds:.3f}s",
        ]

        if leg.requested_price is not None:
            parts.insert(3, f"limit={_format_decimal(leg.requested_price, places=2)}")
        if leg.reference_price is not None:
            parts.insert(3, f"ref={_format_decimal(leg.reference_price, places=2)}")

        line = " | ".join(parts)
        if to_console:
            print(line)
        if logger and not to_console:
            logger.log(line, "INFO")


def _to_decimal(value: Decimal | float | int | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _extract_price_quantity(leg: LegResult) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    price = leg.price
    quantity = leg.quantity
    if price is None or quantity is None:
        return None, None
    try:
        price_dec = _to_decimal(price)
        quantity_dec = abs(_to_decimal(quantity))
    except (InvalidOperation, ValueError):  # pragma: no cover - defensive
        return None, None
    return price_dec, quantity_dec


def _calculate_pair_metrics(leg_a: LegResult, leg_b: LegResult) -> Tuple[Decimal, Decimal]:
    price_a, qty_a = _extract_price_quantity(leg_a)
    price_b, qty_b = _extract_price_quantity(leg_b)

    if price_a is None or price_b is None or qty_a is None or qty_b is None:
        return Decimal("0"), Decimal("0")

    matched_qty = min(qty_a, qty_b)
    if matched_qty <= 0:
        return Decimal("0"), Decimal("0")

    side_a = leg_a.side.lower()
    side_b = leg_b.side.lower()
    valid_sides = {"buy", "sell"}

    if side_a not in valid_sides or side_b not in valid_sides:
        return Decimal("0"), Decimal("0")

    if side_a == side_b:
        # Fallback to cash-flow approach limited to matched quantity
        factor_a = Decimal("1") if side_a == "sell" else Decimal("-1")
        factor_b = Decimal("1") if side_b == "sell" else Decimal("-1")
        pnl = (price_a * matched_qty * factor_a) + (price_b * matched_qty * factor_b)
        volume = (price_a + price_b) * matched_qty
        return pnl, volume

    if side_a == "sell":
        sell_price = price_a
        buy_price = price_b
    else:
        sell_price = price_b
        buy_price = price_a

    pnl = (sell_price - buy_price) * matched_qty
    volume = (sell_price + buy_price) * matched_qty
    return pnl, volume


def _calculate_cycle_pair_metrics(results: List[LegResult]) -> Tuple[Decimal, Decimal]:
    total_pnl = Decimal("0")
    total_volume = Decimal("0")

    leg_pairs: List[Tuple[LegResult, LegResult]] = []
    if len(results) >= 3:
        leg_pairs.append((results[0], results[2]))
    if len(results) >= 4:
        leg_pairs.append((results[1], results[3]))

    if not leg_pairs:
        return total_pnl, total_volume

    for leg_a, leg_b in leg_pairs:
        status_a = (leg_a.status or "").upper()
        status_b = (leg_b.status or "").upper()
        if status_a.startswith("VIRTUAL") or status_b.startswith("VIRTUAL"):
            continue
        pair_pnl, pair_volume = _calculate_pair_metrics(leg_a, leg_b)
        total_pnl += pair_pnl
        total_volume += pair_volume

    return total_pnl, total_volume


def _calculate_cycle_pnl(results: List[LegResult]) -> Decimal:
    pnl, _ = _calculate_cycle_pair_metrics(results)
    return pnl


def _calculate_cycle_volume(results: List[LegResult]) -> Decimal:
    _, volume = _calculate_cycle_pair_metrics(results)
    return volume


def _print_pnl_progress(
    cycle_number: int,
    cycle_pnl: Decimal,
    cumulative_pnl: Decimal,
    cycle_volume: Decimal,
    cumulative_volume: Decimal,
    cycle_duration_seconds: float,
    lighter_balance: Optional[Decimal],
    total_runtime_seconds: float,
    logger: Optional[TradingLogger] = None,
    *,
    to_console: bool = True,
) -> None:
    header = f"PnL Progress After Cycle {cycle_number}"
    if to_console:
        print(header)
        print("-" * len(header))
        print(f"Cycle PnL: {_format_decimal(cycle_pnl, places=2)}")
        print(f"Cumulative PnL: {_format_decimal(cumulative_pnl, places=2)}")
        print(f"Cycle Volume (USD): {_format_decimal(cycle_volume, places=2)}")
        print(f"Cumulative Volume (USD): {_format_decimal(cumulative_volume, places=2)}")
        print(f"Cycle Duration: {cycle_duration_seconds:.2f}s")
        print(f"Runtime Since Start: {_format_duration(total_runtime_seconds)}")
        if lighter_balance is not None:
            print(f"Lighter Available Balance: {_format_decimal(lighter_balance, places=2)}")
        else:
            print("Lighter Available Balance: unavailable")
    if logger and not to_console:
        logger.log(header, "INFO")
        logger.log(f"Cycle PnL: {_format_decimal(cycle_pnl, places=2)}", "INFO")
        logger.log(f"Cumulative PnL: {_format_decimal(cumulative_pnl, places=2)}", "INFO")
        logger.log(f"Cycle Volume (USD): {_format_decimal(cycle_volume, places=2)}", "INFO")
        logger.log(f"Cumulative Volume (USD): {_format_decimal(cumulative_volume, places=2)}", "INFO")
        logger.log(f"Cycle Duration: {cycle_duration_seconds:.2f}s", "INFO")
        logger.log(f"Runtime Since Start: {_format_duration(total_runtime_seconds)}", "INFO")
        if lighter_balance is not None:
            logger.log(f"Lighter Available Balance: {_format_decimal(lighter_balance, places=2)}", "INFO")
        else:
            logger.log("Lighter Available Balance: unavailable", "INFO")


def _print_leaderboard_points(
    cycle_number: int,
    weekly_points: Optional[Decimal],
    total_points: Optional[Decimal],
    logger: Optional[TradingLogger] = None,
    *,
    to_console: bool = True,
) -> None:
    header = f"Lighter Leaderboard Points After Cycle {cycle_number}"
    weekly_text = _format_decimal(weekly_points, places=2) if weekly_points is not None else "N/A"
    total_text = _format_decimal(total_points, places=2) if total_points is not None else "N/A"

    if to_console:
        print(header)
        print("-" * len(header))
        print(f"Weekly Points: {weekly_text}")
        print(f"Total Points: {total_text}")

    if logger and not to_console:
        logger.log(header, "INFO")
        logger.log(f"Weekly Points: {weekly_text}", "INFO")
        logger.log(f"Total Points: {total_text}", "INFO")


async def _async_main(args: argparse.Namespace) -> None:
    env_path = Path(args.env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Env file not found: {env_path.resolve()}")

    dotenv.load_dotenv(env_path)
    await _auto_provision_lighter_credentials(env_path)
    dotenv.load_dotenv(env_path, override=True)

    lighter_quantity_min = args.lighter_quantity_min
    lighter_quantity_max = args.lighter_quantity_max
    lighter_leverage = getattr(args, "lighter_leverage", DEFAULT_LIGHTER_LEVERAGE)
    try:
        lighter_leverage = int(lighter_leverage)
    except (TypeError, ValueError) as exc:
        raise ValueError("--lighter-leverage must be an integer") from exc

    if lighter_leverage < 1 or lighter_leverage > 125:
        raise ValueError("--lighter-leverage must be between 1 and 125 (inclusive)")

    if (lighter_quantity_min is None) != (lighter_quantity_max is None):
        raise ValueError("Both --lighter-quantity-min and --lighter-quantity-max must be provided together")

    if lighter_quantity_min is not None and lighter_quantity_max is not None:
        if lighter_quantity_min <= 0 or lighter_quantity_max <= 0:
            raise ValueError("Lighter quantity range values must be positive")
        if lighter_quantity_min > lighter_quantity_max:
            raise ValueError("--lighter-quantity-min cannot exceed --lighter-quantity-max")

        step = Decimal("0.001")
        try:
            normalized_min = lighter_quantity_min.quantize(step)
            normalized_max = lighter_quantity_max.quantize(step)
        except InvalidOperation as exc:
            raise ValueError("Lighter quantity range must align to 0.001 precision") from exc

        if normalized_min != lighter_quantity_min or normalized_max != lighter_quantity_max:
            raise ValueError("Lighter quantity range must align to 0.001 precision")

        lighter_quantity_min = normalized_min
        lighter_quantity_max = normalized_max

    if args.lighter_quantity is not None:
        lighter_quantity_base = args.lighter_quantity
    elif lighter_quantity_max is not None:
        lighter_quantity_base = lighter_quantity_max
    else:
        lighter_quantity_base = args.quantity

    log_to_console_option = getattr(args, "log_to_console", None)
    if log_to_console_option is None:
        log_to_console_option = True

    depth_override = getattr(args, "aster_maker_depth", DEFAULT_ASTER_MAKER_DEPTH_LEVEL)
    try:
        aster_maker_depth = int(depth_override)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"--aster-maker-depth must be an integer (received: {depth_override})") from exc
    if aster_maker_depth < 1 or aster_maker_depth > 500:
        raise ValueError("--aster-maker-depth must be between 1 and 500 (inclusive)")

    leg1_depth_arg = getattr(args, "aster_leg1_depth", None)
    if leg1_depth_arg is not None and (leg1_depth_arg < 1 or leg1_depth_arg > 500):
        raise ValueError("--aster-leg1-depth must be between 1 and 500 (inclusive)")

    leg3_depth_arg = getattr(args, "aster_leg3_depth", None)
    if leg3_depth_arg is not None and (leg3_depth_arg < 1 or leg3_depth_arg > 500):
        raise ValueError("--aster-leg3-depth must be between 1 and 500 (inclusive)")

    hot_update_url = getattr(args, "hot_update_url", None)
    if hot_update_url is not None:
        hot_update_url = hot_update_url.strip() or None

    config = CycleConfig(
        aster_ticker=args.aster_ticker,
        lighter_ticker=args.lighter_ticker,
        quantity=args.quantity,
        aster_quantity=args.aster_quantity if args.aster_quantity is not None else args.quantity,
        lighter_quantity=lighter_quantity_base,
        lighter_quantity_min=lighter_quantity_min,
        lighter_quantity_max=lighter_quantity_max,
        direction=args.direction,
        randomize_direction=bool(getattr(args, "randomize_direction", False)),
        direction_seed=getattr(args, "direction_seed", None),
        take_profit_pct=args.take_profit,
        slippage_pct=args.slippage,
        max_wait_seconds=args.max_wait,
        lighter_max_wait_seconds=args.lighter_max_wait,
        poll_interval=args.poll_interval,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay,
        max_cycles=max(0, args.cycles),
        delay_between_cycles=max(0.0, args.cycle_delay),
        virtual_aster_maker=args.virtual_aster_maker,
        preserve_initial_position=bool(getattr(args, "preserve_initial_position", False)),
        coordinator_url=getattr(args, "coordinator_url", None),
        coordinator_agent_id=getattr(args, "coordinator_agent", None),
    coordinator_username=getattr(args, "coordinator_username", None),
    coordinator_password=getattr(args, "coordinator_password", None),
        virtual_aster_price_source=args.virtual_maker_price_source,
        virtual_aster_reference_symbol=args.virtual_maker_symbol,
        aster_maker_depth_level=aster_maker_depth,
        aster_leg1_depth_level=leg1_depth_arg,
        aster_leg3_depth_level=leg3_depth_arg,
        hot_update_url=hot_update_url,
        memory_clean_interval_seconds=args.memory_clean_interval,
        memory_warn_mb=args.memory_warn_mb,
        log_to_console=bool(log_to_console_option),
        tracemalloc_enabled=bool(getattr(args, "tracemalloc", False)),
        tracemalloc_top=int(getattr(args, "tracemalloc_top", 15) or 15),
        tracemalloc_group_by=str(getattr(args, "tracemalloc_group_by", "lineno") or "lineno"),
        tracemalloc_filter=getattr(args, "tracemalloc_filter", None),
        tracemalloc_frames=int(getattr(args, "tracemalloc_frames", 25) or 25),
        enforce_min_cycle_interval=not bool(getattr(args, "disable_min_cycle_interval", False)),
        lighter_leverage=lighter_leverage,
    )

    executor = HedgingCycleExecutor(config)
    await executor.setup()
    cycle_index = 0
    cumulative_pnl = Decimal("0")
    cumulative_volume = Decimal("0")
    run_start_time = time.time()
    network_error_count = 0
    network_error_exceptions: Tuple[type[BaseException], ...] = (
        asyncio.TimeoutError,
        ConnectionError,
        OSError,
    )
    try:
        await executor.report_metrics(
            total_cycles=cycle_index,
            cumulative_pnl=cumulative_pnl,
            cumulative_volume=cumulative_volume,
        )
    except Exception:
        pass
    try:
        while True:
            payload: Optional[Dict[str, Any]] = None
            if config.hot_update_url:
                payload = await _wait_for_hot_update_enabled(config.hot_update_url, executor.logger)
                if payload:
                    try:
                        executor.apply_depth_config(payload)
                    except Exception as exc:
                        executor.logger.log(f"Failed to apply hot update config: {exc}", "WARNING")

            await executor.wait_for_resume("before cycle start")

            cycle_index += 1
            depth_levels = {
                "maker": getattr(
                    executor.config,
                    "aster_maker_depth_level",
                    getattr(executor, "_aster_maker_depth_level", None),
                ),
                "leg1": getattr(
                    executor.config,
                    "aster_leg1_depth_level",
                    getattr(executor, "_aster_leg1_depth_level", None),
                ),
                "leg3": getattr(
                    executor.config,
                    "aster_leg3_depth_level",
                    getattr(executor, "_aster_leg3_depth_level", None),
                ),
            }
            depth_display = ", ".join(
                f"{name}={value if value is not None else 'N/A'}"
                for name, value in depth_levels.items()
            )
            executor.logger.log(
                f"Starting hedging cycle #{cycle_index} | depth levels: {depth_display}",
                "INFO",
            )
            cycle_start_time = time.time()
            try:
                await executor.ensure_l1_top_up_if_needed()
                results = await executor.execute_cycle()
            except SkipCycleError as exc:
                network_error_count = 0
                executor.logger.log(str(exc), "WARNING")
                try:
                    await executor.ensure_lighter_flat()
                except Exception as flatten_exc:
                    executor.logger.log(
                        f"Emergency flatten after skipped cycle failed: {flatten_exc}",
                        "ERROR",
                    )

                await executor.report_metrics(
                    total_cycles=cycle_index,
                    cumulative_pnl=cumulative_pnl,
                    cumulative_volume=cumulative_volume,
                )

                sleep_seconds = _compute_cycle_pause_seconds(
                    cycle_start_time,
                    config.delay_between_cycles,
                    enforce_min_interval=executor.config.enforce_min_cycle_interval,
                )
                if sleep_seconds > 0:
                    message = f"Waiting {sleep_seconds:.2f} seconds before next cycle"
                    if executor.config.enforce_min_cycle_interval:
                        message += f" (minimum interval {MIN_CYCLE_INTERVAL_SECONDS:.0f}s enforced)"
                    executor.logger.log(message, "INFO")
                    await asyncio.sleep(sleep_seconds)
                continue
            except network_error_exceptions as exc:
                network_error_count += 1
                executor.logger.log(
                    f"Cycle {cycle_index} aborted due to network error: {exc}",
                    "ERROR",
                )
                try:
                    await executor.ensure_lighter_flat()
                except Exception as flatten_exc:
                    executor.logger.log(
                        f"Emergency flatten after network error failed: {flatten_exc}",
                        "ERROR",
                    )

                await executor.report_metrics(
                    total_cycles=cycle_index,
                    cumulative_pnl=cumulative_pnl,
                    cumulative_volume=cumulative_volume,
                )

                if network_error_count >= 3:
                    executor.logger.log(
                        "Encountered 3 consecutive network errors; stopping execution.",
                        "ERROR",
                    )
                    break

                pause_seconds = max(
                    30.0,
                    _compute_cycle_pause_seconds(
                        cycle_start_time,
                        config.delay_between_cycles,
                        enforce_min_interval=executor.config.enforce_min_cycle_interval,
                    ),
                )
                message = f"Pausing {pause_seconds:.2f} seconds before attempting next cycle"
                if executor.config.enforce_min_cycle_interval:
                    message += f" (minimum interval {MIN_CYCLE_INTERVAL_SECONDS:.0f}s enforced)"
                executor.logger.log(message, "WARNING")
                await asyncio.sleep(pause_seconds)
                continue
            else:
                network_error_count = 0

            cycle_duration = time.time() - cycle_start_time
            total_runtime = time.time() - run_start_time
            executor.logger.log(f"Completed hedging cycle #{cycle_index}", "INFO")
            if getattr(executor, "_cycle_had_timeout", False):
                executor.logger.log(
                    (
                        f"Cycle {cycle_index} experienced Lighter timeout/position-based fill; "
                        f"skipping summary and PnL/volume accumulation to avoid distortion"
                    ),
                    "INFO",
                )
            else:
                _print_summary(
                    results,
                    cycle_number=cycle_index,
                    logger=executor.logger,
                    to_console=bool(getattr(executor.config, "log_to_console", False)),
                )

                cycle_pnl = _calculate_cycle_pnl(results)
                cumulative_pnl += cycle_pnl

                cycle_volume = _calculate_cycle_volume(results)
                cumulative_volume += cycle_volume

                lighter_balance: Optional[Decimal] = None
                if executor.lighter_client:
                    try:
                        lighter_balance = await executor.lighter_client.get_available_balance()
                    except Exception as exc:
                        executor.logger.log(
                            f"Unable to fetch Lighter balance after cycle {cycle_index}: {exc}",
                            "WARNING",
                        )

                _print_pnl_progress(
                    cycle_index,
                    cycle_pnl,
                    cumulative_pnl,
                    cycle_volume,
                    cumulative_volume,
                    cycle_duration,
                    lighter_balance,
                    total_runtime,
                    logger=executor.logger,
                    to_console=bool(getattr(executor.config, "log_to_console", False)),
                )

                await executor._log_leaderboard_points(cycle_index)

            await executor.report_metrics(
                total_cycles=cycle_index,
                cumulative_pnl=cumulative_pnl,
                cumulative_volume=cumulative_volume,
            )

            try:
                await executor.ensure_lighter_flat()
            except Exception as exc:
                executor.logger.log(
                    f"Emergency flatten failed after cycle {cycle_index}: {exc}",
                    "ERROR",
                )

            if config.max_cycles and cycle_index >= config.max_cycles:
                executor.logger.log(
                    f"Reached configured cycle limit ({config.max_cycles}); stopping execution",
                    "INFO",
                )
                break

            sleep_seconds = _compute_cycle_pause_seconds(
                cycle_start_time,
                config.delay_between_cycles,
                enforce_min_interval=executor.config.enforce_min_cycle_interval,
            )
            if sleep_seconds > 0:
                message = f"Waiting {sleep_seconds:.2f} seconds before next cycle"
                if executor.config.enforce_min_cycle_interval:
                    message += f" (minimum interval {MIN_CYCLE_INTERVAL_SECONDS:.0f}s enforced)"
                executor.logger.log(message, "INFO")
                await asyncio.sleep(sleep_seconds)
    finally:
        try:
            await executor.ensure_lighter_flat()
        except Exception as exc:
            executor.logger.log(
                f"Emergency flatten failed during shutdown: {exc}",
                "ERROR",
            )
        try:
            await executor.report_metrics(
                total_cycles=cycle_index,
                cumulative_pnl=cumulative_pnl,
                cumulative_volume=cumulative_volume,
            )
        except Exception:
            pass
        await executor.shutdown()


def main() -> None:
    args = _parse_args()
    _configure_logging(args.log_level)

    if getattr(args, "reset_credentials", False) and getattr(args, "l1_private_key", None):
        logging.error("--reset-credentials cannot be combined with --l1-private-key")
        sys.exit(2)

    new_private_key_arg = getattr(args, "l1_private_key", None)
    if new_private_key_arg:
        env_path = Path(args.env_file)
        env_path.parent.mkdir(parents=True, exist_ok=True)
        if not env_path.exists():
            try:
                env_path.touch()
            except Exception as exc:
                logging.error("Unable to create env file at %s: %s", env_path.resolve(), exc)
                sys.exit(1)

        rotation_logger = logging.getLogger("hedge.rekey")
        rotation_logger.info(
            "CLI provided new L1 wallet private key; rotating credentials in %s", env_path
        )

        normalized_key = _normalize_private_key_hex(str(new_private_key_arg))
        if not normalized_key:
            rotation_logger.error("Provided L1 private key is empty after normalization")
            sys.exit(1)

        _clear_env_credentials(env_path, logger=rotation_logger)
        _persist_env_value(env_path, "L1_WALLET_PRIVATE_KEY", normalized_key)
        _persist_env_value(env_path, "LIGHTER_L1_PRIVATE_KEY", normalized_key)
        rotation_logger.info("Stored new L1 wallet private key from CLI input")

    if getattr(args, "reset_credentials", False):
        env_path = Path(args.env_file)
        reset_logger = logging.getLogger("hedge.reset")
        if not env_path.exists():
            reset_logger.error("Env file not found: %s", env_path.resolve())
            sys.exit(1)

        removed = _clear_env_credentials(env_path, logger=reset_logger)
        if removed:
            reset_logger.info("Selected credentials cleared from %s", env_path)
        else:
            reset_logger.info("No matching credentials found in %s", env_path)
        sys.exit(0)

    try:
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        logging.warning("Hedging cycle interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logging.exception("Hedging cycle failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
