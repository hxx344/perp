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
import logging
import random
import sys
import time
import os
import gc
import tracemalloc
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, cast

import aiohttp
from eth_account import Account
from web3 import Web3
from web3.exceptions import TimeExhausted

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
    max_retries: int
    retry_delay_seconds: float
    max_cycles: int
    delay_between_cycles: float
    virtual_aster_maker: bool
    lighter_quantity_min: Optional[Decimal] = None
    lighter_quantity_max: Optional[Decimal] = None
    virtual_aster_price_source: str = "aster"
    virtual_aster_reference_symbol: Optional[str] = None
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


_LEADERBOARD_ENDPOINT = "/api/v1/leaderboard"


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
        async with session.get(url, params=params) as response:
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
ARBITRUM_USDC_CONTRACT = Web3.to_checksum_address("0xaf88d065e77c8cc2239327c5edb3a432268e5831")
_REQUIRED_LIGHTER_ENV = (
    "API_KEY_PRIVATE_KEY",
    "LIGHTER_ACCOUNT_INDEX",
    "LIGHTER_API_KEY_INDEX",
)
DEFAULT_LIGHTER_LEVERAGE = 50

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
    transfer_fn = contract.functions.transfer(checksum_dest, balance)
    gas_estimate = transfer_fn.estimate_gas({"from": checksum_from})
    tx = transfer_fn.build_transaction(
        {
            "from": checksum_from,
            "nonce": nonce,
            "gas": gas_estimate,
            "gasPrice": gas_price,
            "chainId": ARBITRUM_CHAIN_ID,
        }
    )

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

    def __init__(self, config: CycleConfig):
        self.config = config
        ticker_label = f"{config.aster_ticker}_{config.lighter_ticker}".replace("/", "-")
        self.logger = TradingLogger(
            exchange="hedge",
            ticker=ticker_label,
            log_to_console=bool(getattr(config, "log_to_console", False)),
        )

        self._lighter_quantity_min = config.lighter_quantity_min
        self._lighter_quantity_max = config.lighter_quantity_max
        self._lighter_quantity_step = Decimal("0.001")
        self._current_cycle_lighter_quantity: Optional[Decimal] = None
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
        self._leaderboard_address_warning_emitted = False
        self._cached_leaderboard_points: Optional[Tuple[Optional[Decimal], Optional[Decimal]]] = None
        self._leaderboard_points_cycle: int = 0

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
        elif cycle_number - self._leaderboard_points_cycle >= 20:
            refresh_needed = True

        if refresh_needed:
            try:
                weekly_points, total_points = await _fetch_lighter_leaderboard_points(
                    address,
                    base_url=getattr(self.lighter_client, "base_url", None) if self.lighter_client else None,
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

        self.aster_config.contract_id = aster_contract_id
        self.aster_config.tick_size = aster_tick

        self.logger.log(
            (
                f"Contracts resolved | Aster: id={aster_contract_id}, tick={_format_decimal(aster_tick, 8)}; "
                f"Lighter: id={lighter_contract_id}, tick={_format_decimal(lighter_tick, 8)}"
            ),
            "INFO",
        )

        await self._ensure_lighter_leverage(DEFAULT_LIGHTER_LEVERAGE)

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
        entry_direction = self.config.direction
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
                if attempt >= self.config.max_retries:
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

            target_price = await self._calculate_aster_maker_price(direction)
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
                if attempt >= self.config.max_retries:
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
                target_price = await self._calculate_virtual_maker_price(direction)
            except Exception as exc:  # pragma: no cover - defensive logging
                last_error = str(exc)
                self.logger.log(
                    f"{leg_name} | Virtual pricing attempt {attempt} failed: {last_error}",
                    "ERROR",
                )
                if attempt >= self.config.max_retries:
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
                if attempt >= self.config.max_retries:
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

        max_attempts = min(5, max(1, self.config.max_retries))
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

    async def _calculate_aster_maker_price(self, direction: str) -> Decimal:
        if not self._has_aster_market_data():
            raise RuntimeError("Aster market data source is not available")

        tick = self.aster_config.tick_size if self.aster_config.tick_size > 0 else Decimal("0")
        level = 4

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

    async def _calculate_virtual_maker_price(self, direction: str) -> Decimal:
        if self._virtual_price_source == "bn" and self._binance_price_client:
            return await self._calculate_binance_maker_price(direction)
        return await self._calculate_aster_maker_price(direction)

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

        position = await self.lighter_client.get_account_positions()
        if position == 0:
            self.logger.log("No Lighter position detected; no emergency action required", "INFO")
            # Add a blank line for readability before the next cycle output
            if bool(getattr(self.config, "log_to_console", False)):
                print()
            return

        # Positive values denote a net long position; negative values denote a net short position.
        side = "sell" if position > 0 else "buy"
        quantity = abs(position)

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

        if not self.lighter_config.contract_id:
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
                expected_final_position=Decimal("0"),
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
        help="Maximum number of retries for Aster maker orders before aborting",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=5.0,
        help="Delay in seconds before retrying an Aster maker order",
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
        "--cycles",
        type=int,
        default=0,
        help="Number of hedging cycles to run (0 means run continuously until interrupted)",
    )
    parser.add_argument(
        "--cycle-delay",
        type=float,
        default=0.0,
        help="Optional delay in seconds between successive hedging cycles",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the environment file containing both exchange credentials",
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

    config = CycleConfig(
        aster_ticker=args.aster_ticker,
        lighter_ticker=args.lighter_ticker,
        quantity=args.quantity,
        aster_quantity=args.aster_quantity if args.aster_quantity is not None else args.quantity,
        lighter_quantity=lighter_quantity_base,
        lighter_quantity_min=lighter_quantity_min,
        lighter_quantity_max=lighter_quantity_max,
        direction=args.direction,
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
        virtual_aster_price_source=args.virtual_maker_price_source,
        virtual_aster_reference_symbol=args.virtual_maker_symbol,
        memory_clean_interval_seconds=args.memory_clean_interval,
        memory_warn_mb=args.memory_warn_mb,
    log_to_console=bool(log_to_console_option),
        tracemalloc_enabled=bool(getattr(args, "tracemalloc", False)),
        tracemalloc_top=int(getattr(args, "tracemalloc_top", 15) or 15),
        tracemalloc_group_by=str(getattr(args, "tracemalloc_group_by", "lineno") or "lineno"),
        tracemalloc_filter=getattr(args, "tracemalloc_filter", None),
        tracemalloc_frames=int(getattr(args, "tracemalloc_frames", 25) or 25),
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
        while True:
            cycle_index += 1
            executor.logger.log(f"Starting hedging cycle #{cycle_index}", "INFO")
            cycle_start_time = time.time()
            try:
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

                if config.delay_between_cycles > 0:
                    executor.logger.log(
                        f"Waiting {config.delay_between_cycles} seconds before next cycle",
                        "INFO",
                    )
                    await asyncio.sleep(config.delay_between_cycles)
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

                if network_error_count >= 3:
                    executor.logger.log(
                        "Encountered 3 consecutive network errors; stopping execution.",
                        "ERROR",
                    )
                    break

                pause_seconds = 30
                executor.logger.log(
                    f"Pausing {pause_seconds} seconds before attempting next cycle",
                    "WARNING",
                )
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

            if config.delay_between_cycles > 0:
                executor.logger.log(
                    f"Waiting {config.delay_between_cycles} seconds before next cycle",
                    "INFO",
                )
                await asyncio.sleep(config.delay_between_cycles)
    finally:
        try:
            await executor.ensure_lighter_flat()
        except Exception as exc:
            executor.logger.log(
                f"Emergency flatten failed during shutdown: {exc}",
                "ERROR",
            )
        await executor.shutdown()


def main() -> None:
    args = _parse_args()
    _configure_logging(args.log_level)

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
