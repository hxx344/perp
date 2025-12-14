#!/usr/bin/env python3
"""Paradex account monitor.

This helper polls the Paradex account using L2 credentials only, summarises PnL
and balance, and forwards the snapshot to the hedge coordinator so the dashboard
can display per-agent health.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

try:  # pragma: no cover - optional dependency wiring
    from paradex_py import Paradex  # type: ignore
    from paradex_py.environment import PROD, TESTNET  # type: ignore
    from starknet_py.common import int_from_hex  # type: ignore
except ImportError:  # pragma: no cover - dependency guard
    Paradex = None  # type: ignore
    PROD = None  # type: ignore
    TESTNET = None  # type: ignore
    int_from_hex = None  # type: ignore

getcontext().prec = 28

LOGGER = logging.getLogger("monitor.paradex_accounts")
DEFAULT_POLL_SECONDS = 15.0
DEFAULT_TIMEOUT_SECONDS = 10.0
MAX_ACCOUNT_POSITIONS = 12
BALANCE_TOTAL_PATHS: Tuple[Tuple[str, ...], ...] = (
    ("total", "USDT"),
    ("total", "usdt"),
    ("total", "USD"),
    ("total", "usd"),
    ("total",),
    ("info", "accountBalance"),
    ("info", "account_balance"),
    ("info", "walletBalance"),
    ("info", "wallet_balance"),
    ("info", "equity"),
    ("balance",),
    ("equity",),
    ("account_value",),
    ("accountValue",),
    ("accountValueUsd",),
    ("account_value_usd",),
    ("total_account_value",),
    ("totalAccountValue",),
    ("equity_value",),
    ("equityValue",),
    ("equity_value_usd",),
    ("equityUsd",),
    ("equity_usd",),
    ("cash_balance",),
    ("cashBalance",),
    ("USDT",),
    ("usdt",),
)
BALANCE_AVAILABLE_PATHS: Tuple[Tuple[str, ...], ...] = (
    ("free", "USDT"),
    ("free", "usdt"),
    ("free", "USD"),
    ("free", "usd"),
    ("available", "USDT"),
    ("available", "usdt"),
    ("available", "USD"),
    ("available", "usd"),
    ("available_balance",),
    ("info", "availableBalance"),
    ("info", "available_balance"),
    ("info", "availableMargin"),
    ("info", "available_margin"),
    ("freeCollateral",),
    ("free_collateral",),
    ("freeCollateralUsd",),
    ("free_collateral_usd",),
    ("available_equity",),
    ("availableEquity",),
)


def load_env_files(paths: Sequence[str]) -> None:
    if not paths:
        return
    for raw_path in paths:
        if not raw_path:
            continue
        env_path = Path(raw_path).expanduser()
        if not env_path.exists():
            LOGGER.debug("Env file %s not found; skipping", env_path)
            continue
        try:
            with env_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    if "=" not in stripped:
                        continue
                    key, value = stripped.split("=", 1)
                    key = key.strip()
                    if not key:
                        continue
                    value = value.strip()
                    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                        value = value[1:-1]
                    if key not in os.environ:
                        os.environ[key] = value
        except Exception as exc:  # pragma: no cover - best effort env loading
            LOGGER.warning("Failed to load env file %s: %s", env_path, exc)
            continue
        LOGGER.info("Loaded environment variables from %s", env_path)


@dataclass(frozen=True)
class ParadexCredentials:
    label: str
    l1_address: str
    l2_private_key_hex: str
    environment: str = "prod"
    l2_address: Optional[str] = None


def decimal_from(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return Decimal(text)
        except InvalidOperation:
            return None
    return None


def decimal_to_str(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    try:
        return format(value, "f")
    except Exception:
        return str(value)


def extract_from_paths(source: Dict[str, Any], *paths: Sequence[str]) -> Optional[Any]:
    for path in paths:
        cursor: Any = source
        valid = True
        for key in path:
            if isinstance(cursor, dict) and key in cursor:
                cursor = cursor[key]
            else:
                valid = False
                break
        if valid and cursor is not None:
            return cursor
    return None


def extract_nested_balance(source: Dict[str, Any], paths: Tuple[Tuple[str, ...], ...]) -> Optional[Decimal]:
    if not isinstance(source, dict):
        return None
    nested_blocks: Tuple[str, ...] = (
        "data",
        "result",
        "info",
        "account",
        "balance",
        "balances",
    )
    for key in nested_blocks:
        block = source.get(key)
        if isinstance(block, dict):
            value = extract_from_paths(block, *paths)
            if value is not None:
                return decimal_from(value)
        elif isinstance(block, list):
            for entry in block:
                if not isinstance(entry, dict):
                    continue
                value = extract_from_paths(entry, *paths)
                if value is not None:
                    return decimal_from(value)
    return None


def guess_symbol(entry: Dict[str, Any]) -> str:
    fields = [
        entry.get("symbol"),
        entry.get("market"),
        entry.get("instrument"),
        entry.get("asset"),
        extract_from_paths(entry, ("info", "instrument")),
        extract_from_paths(entry, ("info", "symbol")),
    ]
    for candidate in fields:
        if candidate:
            return str(candidate)
    return ""


def base_asset(symbol: str) -> Optional[str]:
    if not symbol:
        return None
    text = str(symbol).strip().upper()
    if not text:
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
    return candidate or None


def normalize_side(entry: Dict[str, Any], size: Optional[Decimal]) -> Optional[str]:
    side_fields = [
        entry.get("side"),
        entry.get("position_side"),
        entry.get("positionSide"),
        entry.get("direction"),
        extract_from_paths(entry, ("info", "side")),
        extract_from_paths(entry, ("info", "direction")),
    ]
    for candidate in side_fields:
        if candidate:
            text = str(candidate).strip().lower()
            if text in {"long", "buy", "bid"}:
                return "LONG"
            if text in {"short", "sell", "ask"}:
                return "SHORT"
    if size is not None and size < 0:
        return "SHORT"
    if size is not None and size > 0:
        return "LONG"
    return None


def determine_signed_size(size: Optional[Decimal], side: Optional[str]) -> Optional[Decimal]:
    if size is None:
        return None
    if side == "SHORT" and size > 0:
        return -size
    if side == "LONG" and size < 0:
        return size
    return size


def compute_position_pnl(entry: Dict[str, Any]) -> Tuple[Decimal, Dict[str, Any], Optional[Decimal]]:
    size_candidates = [
        ("contracts",),
        ("size",),
        ("amount",),
        ("positionAmt",),
        ("net_size",),
        ("position_size",),
        ("positionSize",),
        ("position_qty",),
        ("positionQty",),
        ("netSize",),
        ("quantity",),
        ("info", "position_size"),
        ("info", "contracts"),
        ("info", "size"),
        ("info", "positionQty"),
    ]
    entry_price_candidates = [
        ("entry_price",),
        ("entryPrice",),
        ("average_price",),
        ("average_entry_price",),
        ("avg_entry_price",),
        ("avgEntryPrice",),
        ("avgEntry",),
        ("averageEntryPrice",),
        ("averageEntry",),
        ("entryPx",),
        ("avgPrice",),
        ("info", "entry_price"),
        ("info", "average_entry_price"),
        ("info", "avgEntryPrice"),
        ("info", "avg_entry_price"),
        ("info", "entryPx"),
    ]
    mark_price_candidates = [
        ("mark_price",),
        ("markPrice",),
        ("last_price",),
        ("lastPrice",),
        ("index_price",),
        ("indexPrice",),
        ("oracle_price",),
        ("oraclePrice",),
        ("lastTradedPrice",),
        ("last_traded_price",),
        ("price",),
        ("info", "mark_price"),
        ("info", "markPrice"),
        ("info", "last_price"),
        ("info", "lastPrice"),
        ("info", "index_price"),
        ("info", "indexPrice"),
        ("info", "oracle_price"),
        ("info", "oraclePrice"),
        ("info", "lastTradedPrice"),
        ("info", "last_traded_price"),
        ("info", "price"),
    ]
    pnl_candidates = [
        ("unrealizedPnl",),
        ("unrealized_pnl",),
        ("pnl",),
        ("unrealizedPnL",),
        ("unrealized_pnl_usd",),
        ("unrealizedPnlUsd",),
        ("profit",),
        ("info", "unrealizedPnL"),
        ("info", "unrealizedPnl"),
        ("info", "unrealized_pnl"),
        ("info", "pnl"),
        ("info", "profit"),
    ]

    raw_size = extract_from_paths(entry, *size_candidates)
    size_value = decimal_from(raw_size)
    side = normalize_side(entry, size_value)
    signed_size = determine_signed_size(size_value, side)

    raw_entry = extract_from_paths(entry, *entry_price_candidates)
    entry_price = decimal_from(raw_entry)
    raw_mark = extract_from_paths(entry, *mark_price_candidates)
    mark_price = decimal_from(raw_mark)
    raw_pnl = extract_from_paths(entry, *pnl_candidates)
    pnl_value = decimal_from(raw_pnl)

    if mark_price is None and entry_price is not None:
        mark_price = entry_price
    if entry_price is None and mark_price is not None:
        entry_price = mark_price

    if pnl_value is None and None not in (signed_size, entry_price, mark_price):
        try:
            pnl_value = (mark_price - entry_price) * signed_size  # type: ignore[arg-type]
        except Exception:
            pnl_value = Decimal("0")

    if pnl_value is None:
        pnl_value = Decimal("0")

    payload = {
        "symbol": guess_symbol(entry) or "--",
        "side": side,
        "net_size": decimal_to_str(signed_size),
        "entry_price": decimal_to_str(entry_price),
        "mark_price": decimal_to_str(mark_price),
        "pnl": decimal_to_str(pnl_value),
    }
    return pnl_value, payload, signed_size


def load_single_account(*, label: str) -> ParadexCredentials:
    l1_address = os.getenv("PARADEX_L1_ADDRESS")
    l2_private_key_hex = os.getenv("PARADEX_L2_PRIVATE_KEY")
    if not l1_address or not l2_private_key_hex:
        raise ValueError(
            "Missing PARADEX_L1_ADDRESS / PARADEX_L2_PRIVATE_KEY. "
            "Populate them in your environment or .env file."
        )
    environment = (os.getenv("PARADEX_ENVIRONMENT") or "prod").strip().lower()
    l2_address = os.getenv("PARADEX_L2_ADDRESS") or None
    return ParadexCredentials(
        label=label.strip() or "default",
        l1_address=l1_address.strip(),
        l2_private_key_hex=l2_private_key_hex.strip(),
        environment=environment,
        l2_address=(l2_address.strip() if l2_address else None),
    )


def build_paradex_client(creds: ParadexCredentials) -> Any:
    """Construct Paradex client with optional RPC version override.

    PARADEX_RPC_VERSION env var (e.g. v0_9) is passed through to starknet fullnode RPC.
    This helps avoid "Invalid block id" errors when the node requires an explicit versioned
    RPC path.
    """
    if Paradex is None or TESTNET is None or PROD is None or int_from_hex is None:
        raise ImportError(
            "para_account_monitor requires paradex-py (and starknet-py) packages. "
            "Install them before running the monitor."
        )
    env_map = {
        "prod": PROD,
        "production": PROD,
        "mainnet": PROD,
        "testnet": TESTNET,
        "nightly": TESTNET,
    }
    env = env_map.get(creds.environment.lower(), PROD)
    try:
        l2_private_key = int_from_hex(creds.l2_private_key_hex)
    except Exception as exc:  # pragma: no cover - user input validation
        raise ValueError(f"Invalid PARADEX_L2_PRIVATE_KEY: {exc}") from exc

    rpc_version = (os.getenv("PARADEX_RPC_VERSION") or "").strip() or None
    client = Paradex(env=env, logger=None)
    client.init_account(
        l1_address=creds.l1_address,
        l2_private_key=l2_private_key,
        rpc_version=rpc_version,
    )
    return client


class ParadexAccountMonitor:
    def __init__(
        self,
        *,
        label: str,
        paradex_client: Any,
        coordinator_url: str,
        agent_id: str,
        poll_interval: float,
        request_timeout: float,
        max_positions: int,
        coordinator_username: Optional[str] = None,
        coordinator_password: Optional[str] = None,
        default_market: Optional[str] = None,
    ) -> None:
        self._label = label
        self._client = paradex_client
        self._coordinator_url = coordinator_url.rstrip("/")
        self._agent_id = agent_id
        self._poll_interval = max(poll_interval, 2.0)
        self._timeout = max(request_timeout, 1.0)
        self._max_positions = max(1, max_positions)
        self._http = requests.Session()
        username = (coordinator_username or "").strip()
        password = (coordinator_password or "").strip()
        self._auth: Optional[HTTPBasicAuth] = None
        if username or password:
            self._auth = HTTPBasicAuth(username, password)
            self._http.auth = self._auth
        self._update_endpoint = f"{self._coordinator_url}/update"
        self._control_endpoint = f"{self._coordinator_url}/control"
        self._ack_endpoint = f"{self._coordinator_url}/para/adjust/ack"
        self._default_market = (default_market or os.getenv("PARADEX_DEFAULT_MARKET") or "").strip() or None
        self._symbol_aliases: Dict[str, str] = {}
        self._latest_positions: Dict[str, Decimal] = {}
        self._processed_adjustments: Dict[str, Dict[str, Any]] = {}
        self._register_symbol_hint(self._default_market)

    @staticmethod
    def _normalize_symbol_label(value: Optional[str]) -> str:
        if value is None:
            return ""
        try:
            text = str(value).strip().upper()
        except Exception:
            return ""
        for token in ("/", "-", ":", "_", " "):
            text = text.replace(token, "")
        return text

    def _register_symbol_hint(self, symbol: Optional[str]) -> None:
        if not symbol:
            return
        normalized = self._normalize_symbol_label(symbol)
        if not normalized:
            return
        if normalized not in self._symbol_aliases:
            self._symbol_aliases[normalized] = symbol

    def _record_position(self, symbol: Optional[str], signed_size: Optional[Decimal]) -> None:
        if symbol is None or signed_size is None:
            return
        normalized = self._normalize_symbol_label(symbol)
        if not normalized:
            return
        self._register_symbol_hint(symbol)
        self._latest_positions[normalized] = signed_size

    def _update_cached_position(self, symbol: Optional[str], signed_size: Decimal) -> None:
        if symbol is None:
            return
        normalized = self._normalize_symbol_label(symbol)
        if not normalized:
            return
        self._register_symbol_hint(symbol)
        self._latest_positions[normalized] = signed_size

    def _lookup_net_position(self, symbol: str) -> Optional[Decimal]:
        normalized = self._normalize_symbol_label(symbol)
        if normalized and normalized in self._latest_positions:
            return self._latest_positions.get(normalized)
        self._refresh_position_cache()
        if normalized and normalized in self._latest_positions:
            return self._latest_positions.get(normalized)
        return None

    def _refresh_position_cache(self) -> None:
        positions = self._fetch_positions()
        self._latest_positions = {}
        for entry in positions:
            _, payload, signed_size = compute_position_pnl(entry)
            self._record_position(payload.get("symbol"), signed_size)

    def _resolve_symbol(self, requested_symbols: Optional[Iterable[str]]) -> Optional[str]:
        resolved: Optional[str] = None
        normalized_targets: List[str] = []
        saw_all = False
        symbols_iter: Iterable[str]
        if isinstance(requested_symbols, str):
            symbols_iter = [requested_symbols]
        else:
            symbols_iter = requested_symbols or []
        for candidate in symbols_iter:
            normalized = self._normalize_symbol_label(candidate)
            if not normalized:
                continue
            if normalized in {"ALL", "__ALL__"}:
                saw_all = True
            normalized_targets.append(normalized)
        for normalized in normalized_targets:
            hint = self._symbol_aliases.get(normalized)
            if hint:
                resolved = hint
                break
        if resolved:
            return resolved
        if saw_all and self._default_market:
            return self._default_market
        if normalized_targets and not resolved:
            if self._default_market:
                return self._default_market
        if not resolved and self._symbol_aliases:
            return next(iter(self._symbol_aliases.values()))
        return self._default_market

    def _fetch_positions(self) -> List[Dict[str, Any]]:
        try:
            response = self._client.api_client.fetch_positions()
            if isinstance(response, dict):
                results = response.get("results")
                if isinstance(results, list):
                    return results
            if isinstance(response, list):
                return response
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Failed to fetch Paradex positions for %s: %s", self._label, exc)
        return []

    def _fetch_market_summaries(self) -> List[Dict[str, Any]]:
        api_client = getattr(self._client, "api_client", None)
        if api_client is None:
            return []
        method = getattr(api_client, "fetch_markets_summary", None)
        if method is None:
            return []
        try:
            response = method({"market": "ALL"})  # type: ignore[arg-type]
        except TypeError:
            try:
                response = method()
            except Exception as exc:  # pragma: no cover - network path
                LOGGER.warning("Failed to fetch Paradex market summaries for %s: %s", self._label, exc)
                return []
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Failed to fetch Paradex market summaries for %s: %s", self._label, exc)
            return []
        if isinstance(response, dict):
            results = response.get("results")
            if isinstance(results, list):
                return results
        if isinstance(response, list):
            return response
        return []

    def _build_mark_price_map(self) -> Dict[str, Decimal]:
        price_map: Dict[str, Decimal] = {}
        for entry in self._fetch_market_summaries():
            if not isinstance(entry, dict):
                continue
            market = entry.get("market") or entry.get("symbol") or entry.get("ticker")
            mark_price = decimal_from(
                entry.get("mark_price")
                or entry.get("markPrice")
                or entry.get("last_traded_price")
                or entry.get("lastTradedPrice")
                or entry.get("price")
            )
            if market is None or mark_price is None:
                continue
            normalized = self._normalize_symbol_label(market)
            if not normalized:
                continue
            self._register_symbol_hint(str(market))
            price_map[normalized] = mark_price
        return price_map

    def _fetch_balances(self) -> Dict[str, Any]:
        candidate_methods = ("fetch_balances", "fetch_balance", "fetch_account", "fetch_accounts")
        api_client = getattr(self._client, "api_client", None)
        if api_client is None:
            return {}
        for name in candidate_methods:
            method = getattr(api_client, name, None)
            if method is None:
                continue
            try:
                payload = method()
            except Exception as exc:  # pragma: no cover - network path
                LOGGER.debug("Paradex %s call failed: %s", name, exc)
                continue
            if isinstance(payload, dict):
                LOGGER.info("[DEBUG] raw balance payload via %s: %s", name, payload)
                return payload
            if isinstance(payload, list) and payload:
                first = payload[0]
                if isinstance(first, dict):
                    LOGGER.info("[DEBUG] raw balance payload via %s (list[0]): %s", name, first)
                    return first
        return {}

    def _parse_balances(self, balance_payload: Dict[str, Any]) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        balance_total: Optional[Decimal] = None
        balance_available: Optional[Decimal] = None

        if isinstance(balance_payload, dict):
            total_raw = extract_from_paths(balance_payload, *BALANCE_TOTAL_PATHS)
            balance_total = decimal_from(total_raw)
            if balance_total is None:
                balance_total = extract_nested_balance(balance_payload, BALANCE_TOTAL_PATHS)
            if balance_total is None:
                totals_block = balance_payload.get("total")
                if isinstance(totals_block, dict):
                    for key in ("USDT", "USD", "usdt", "usd"):
                        balance_total = decimal_from(totals_block.get(key))
                        if balance_total is not None:
                            break
            if balance_total is None:
                results_block = balance_payload.get("results")
                if isinstance(results_block, list):
                    for entry in results_block:
                        if not isinstance(entry, dict):
                            continue
                        token = str(entry.get("token") or "").upper()
                        size_value = decimal_from(entry.get("size") or entry.get("balance") or entry.get("equity"))
                        if size_value is None:
                            continue
                        if token in {"USDC", "USDT", "USD"}:
                            balance_total = size_value
                            break
                        if balance_total is None:
                            balance_total = size_value
            available_raw = extract_from_paths(balance_payload, *BALANCE_AVAILABLE_PATHS)
            balance_available = decimal_from(available_raw)
            if balance_available is None:
                balance_available = extract_nested_balance(balance_payload, BALANCE_AVAILABLE_PATHS)
            if balance_available is None:
                free_block = balance_payload.get("free")
                if isinstance(free_block, dict):
                    for key in ("USDT", "USD", "usdt", "usd"):
                        balance_available = decimal_from(free_block.get(key))
                        if balance_available is not None:
                            break
            if balance_available is None:
                results_block = balance_payload.get("results")
                if isinstance(results_block, list):
                    for entry in results_block:
                        if not isinstance(entry, dict):
                            continue
                        token = str(entry.get("token") or "").upper()
                        size_value = decimal_from(entry.get("size") or entry.get("balance") or entry.get("equity"))
                        if size_value is None:
                            continue
                        if token in {"USDC", "USDT", "USD"}:
                            balance_available = size_value
                            break
                        if balance_available is None:
                            balance_available = size_value

        if balance_total is None and balance_available is not None:
            balance_total = balance_available
        if balance_available is None and balance_total is not None:
            balance_available = balance_total

        return balance_total, balance_available

    def _collect(self) -> Optional[Dict[str, Any]]:
        timestamp = time.time()
        positions = self._fetch_positions()

        account_total = Decimal("0")
        account_eth = Decimal("0")
        account_btc = Decimal("0")
        position_rows: List[Dict[str, Any]] = []
        price_map = self._build_mark_price_map()

        for raw_position in positions:
            pnl_value, position_payload, signed_size = compute_position_pnl(raw_position)

            symbol = position_payload.get("symbol")
            normalized_symbol = self._normalize_symbol_label(symbol)
            mark_override = price_map.get(normalized_symbol) if normalized_symbol else None
            if position_payload.get("mark_price") is None and mark_override is not None:
                position_payload["mark_price"] = decimal_to_str(mark_override)

            entry_val = decimal_from(position_payload.get("entry_price"))
            mark_val = decimal_from(position_payload.get("mark_price")) or mark_override
            if (pnl_value is None or pnl_value == 0) and None not in (signed_size, entry_val, mark_val):
                try:
                    pnl_value = (mark_val - entry_val) * signed_size  # type: ignore[arg-type]
                except Exception:
                    pass
            if pnl_value is None:
                pnl_value = Decimal("0")
            position_payload["pnl"] = decimal_to_str(pnl_value)

            account_total += pnl_value
            base = base_asset(position_payload.get("symbol", ""))
            if base == "ETH":
                account_eth += pnl_value
            elif base == "BTC":
                account_btc += pnl_value
            position_rows.append(position_payload)
            self._record_position(symbol, signed_size if signed_size is not None else decimal_from(position_payload.get("net_size")))

        position_rows = position_rows[: self._max_positions]

        balance_payload = self._fetch_balances()
        balance_total, balance_available = self._parse_balances(balance_payload)

        equity_total: Optional[Decimal] = None
        equity_available: Optional[Decimal] = None
        if balance_total is not None:
            equity_total = balance_total + account_total
        if balance_available is not None:
            equity_available = balance_available + account_total

        summary = {
            "account_count": 1,
            "total_pnl": decimal_to_str(account_total),
            "eth_pnl": decimal_to_str(account_eth),
            "btc_pnl": decimal_to_str(account_btc),
            "balance": decimal_to_str(balance_total),
            "available_balance": decimal_to_str(balance_available),
            "equity": decimal_to_str(equity_total),
            "available_equity": decimal_to_str(equity_available),
            "updated_at": timestamp,
        }

        payload = {
            "agent_id": self._agent_id,
            "instrument": f"PARADEX {self._label}",
            "paradex_accounts": {
                "updated_at": timestamp,
                "summary": summary,
                "accounts": [
                    {
                        "name": self._label,
                        "total_pnl": decimal_to_str(account_total),
                        "eth_pnl": decimal_to_str(account_eth),
                        "btc_pnl": decimal_to_str(account_btc),
                        "balance": decimal_to_str(balance_total),
                        "available_balance": decimal_to_str(balance_available),
                        "equity": decimal_to_str(equity_total),
                        "available_equity": decimal_to_str(equity_available),
                        "positions": position_rows,
                        "updated_at": timestamp,
                    }
                ],
            },
        }

        account_obj = getattr(self._client, "account", None)
        transfer_defaults: Dict[str, Any] = {}
        if account_obj is not None:
            try:
                transfer_defaults["l2_address"] = hex(getattr(account_obj, "l2_address"))
            except Exception:
                pass
            transfer_defaults.setdefault("transfer_type", "l2")
        if transfer_defaults:
            payload["paradex_accounts"]["transfer_defaults"] = transfer_defaults
        return payload

    def _push(self, payload: Dict[str, Any]) -> None:
        try:
            response = self._http.post(
                self._update_endpoint,
                json=payload,
                timeout=self._timeout,
                auth=self._auth,
            )
        except RequestException as exc:  # pragma: no cover - network path
            raise RuntimeError(f"Failed to push monitor payload: {exc}") from exc
        if response.status_code >= 400:
            raise RuntimeError(f"Coordinator rejected payload: HTTP {response.status_code} {response.text}")

    def _fetch_agent_control(self) -> Optional[Dict[str, Any]]:
        try:
            response = self._http.get(
                self._control_endpoint,
                params={"agent_id": self._agent_id},
                timeout=self._timeout,
                auth=self._auth,
            )
        except RequestException as exc:
            LOGGER.debug("Failed to query coordinator control endpoint: %s", exc)
            return None
        if response.status_code >= 400:
            LOGGER.debug(
                "Coordinator control query failed: HTTP %s %s",
                response.status_code,
                response.text,
            )
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _place_market_order(self, symbol: str, side: str, quantity: Decimal) -> Any:
        if quantity <= 0:
            raise ValueError("Order quantity must be positive")
        from paradex_py.common.order import Order, OrderType, OrderSide  # type: ignore

        order_side = OrderSide.Buy if side.lower() == "buy" else OrderSide.Sell
        order = Order(
            market=symbol,
            order_type=OrderType.Market,
            order_side=order_side,
            size=quantity,
        )
        return self._client.api_client.submit_order(order)

    def _execute_transfer(self, entry: Dict[str, Any]) -> Tuple[str, str]:
        if not self._client or not getattr(self._client, "account", None):
            raise RuntimeError("Paradex account client not initialized")

        payload_raw = entry.get("payload")
        payload = payload_raw if isinstance(payload_raw, dict) else {}
        amount = decimal_from(payload.get("num_tokens") or payload.get("amount") or entry.get("magnitude"))
        if amount is None or amount <= 0:
            raise ValueError("Transfer amount must be positive")

        target_address_fields = (
            payload.get("target_l2_address"),
            payload.get("recipient"),
            payload.get("recipient_address"),
            payload.get("to_address"),
        )
        target_address = next((str(value).strip() for value in target_address_fields if value), "")
        if not target_address:
            raise ValueError("target_l2_address is required for Paradex transfer")

        reason = None
        metadata = payload.get("transfer_metadata") if isinstance(payload.get("transfer_metadata"), dict) else {}
        if metadata:
            reason = metadata.get("reason")

        try:
            transfer_coro = self._client.account.transfer_on_l2(target_address, amount)
            if asyncio.iscoroutine(transfer_coro):
                self._run_coro_sync(transfer_coro)
            else:
                # paradex-py 可能未来改为同步接口，直接调用即可
                transfer_coro  # type: ignore[misc]
        except Exception as exc:
            raise RuntimeError(f"Paradex L2 transfer failed: {exc}") from exc

        note_parts = [
            f"sent {decimal_to_str(amount) or amount} to {target_address}",
        ]
        if reason:
            note_parts.append(f"reason={reason}")
        return "succeeded", "; ".join(note_parts)

    @staticmethod
    def _run_coro_sync(coro: Awaitable[Any]) -> Any:
        """Run an awaitable from a sync context, reusing event loop when possible."""
        if not asyncio.iscoroutine(coro):
            async def _wrapper() -> Any:
                return await coro
            coro = _wrapper()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)  # type: ignore[arg-type]

        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, loop)  # type: ignore[arg-type]
            return future.result()

        return loop.run_until_complete(coro)

    def _execute_adjustment(self, entry: Dict[str, Any]) -> Tuple[str, str]:
        action = str(entry.get("action", "")).strip().lower()
        if action not in {"add", "reduce"}:
            raise ValueError(f"Unsupported adjustment action '{action}'")
        magnitude = decimal_from(entry.get("magnitude"))
        if magnitude is None or magnitude <= 0:
            raise ValueError(f"Invalid adjustment magnitude '{entry.get('magnitude')}'")
        target_symbols = entry.get("symbols") or entry.get("target_symbols")
        symbol = self._resolve_symbol(target_symbols)
        if not symbol:
            raise ValueError("Unable to resolve symbol for adjustment request")
        net_size = self._lookup_net_position(symbol)
        if net_size is None:
            net_size = Decimal("0")
        if net_size == 0:
            raise ValueError("Current position is flat; cannot determine direction for adjustment")

        trade_quantity = magnitude
        note_suffix: Optional[str] = None
        if action == "add":
            side = "buy" if net_size > 0 else "sell"
        else:  # reduce
            max_reducible = abs(net_size)
            if max_reducible == 0:
                raise ValueError("No exposure available to reduce")
            if magnitude > max_reducible:
                trade_quantity = max_reducible
                note_suffix = (
                    f"requested {decimal_to_str(magnitude) or magnitude} exceeded exposure; "
                    f"clamped to {decimal_to_str(trade_quantity) or trade_quantity}"
                )
            side = "sell" if net_size > 0 else "buy"
            if trade_quantity <= 0:
                raise ValueError("Reduce request resolved to zero size")

        order = self._place_market_order(symbol, side, trade_quantity)
        order_id = None
        if isinstance(order, dict):
            order_id = order.get("id") or order.get("order_id")
        delta = trade_quantity if side == "buy" else -trade_quantity
        new_net = net_size + delta
        self._update_cached_position(symbol, new_net)
        note_core = (
            f"{side.upper()} {decimal_to_str(trade_quantity) or trade_quantity} {symbol} "
            f"(net {decimal_to_str(net_size) or net_size} -> {decimal_to_str(new_net) or new_net})"
        )
        if note_suffix:
            note_core = f"{note_core}; {note_suffix}"
        return "succeeded", note_core if order_id is None else f"{note_core}; order_id={order_id}"

    def _acknowledge_adjustment(self, request_id: str, status: str, note: Optional[str]) -> bool:
        payload = {
            "request_id": request_id,
            "agent_id": self._agent_id,
            "status": status,
        }
        if note is not None:
            payload["note"] = note
        try:
            response = self._http.post(
                self._ack_endpoint,
                json=payload,
                timeout=self._timeout,
                auth=self._auth,
            )
        except RequestException as exc:
            LOGGER.warning("Adjustment ACK request failed for %s: %s", request_id, exc)
            return False
        if response.status_code >= 400:
            LOGGER.warning(
                "Coordinator rejected ACK for %s: HTTP %s %s",
                request_id,
                response.status_code,
                response.text,
            )
            return False
        return True

    def _process_adjustments(self) -> None:
        snapshot = self._fetch_agent_control()
        if not snapshot:
            return
        agent_block = snapshot.get("agent")
        if not isinstance(agent_block, dict):
            return
        pending = agent_block.get("pending_adjustments")
        if not isinstance(pending, list) or not pending:
            self._prune_processed_adjustments()
            return
        for entry in pending:
            if not isinstance(entry, dict):
                continue
            request_id = entry.get("request_id")
            if not request_id:
                continue
            cache = self._processed_adjustments.get(request_id)
            if cache:
                if not cache.get("acked"):
                    acked = self._acknowledge_adjustment(
                        request_id,
                        cache.get("status", "acknowledged"),
                        cache.get("note"),
                    )
                    if acked:
                        cache["acked"] = True
                        cache["timestamp"] = time.time()
                continue
            status = "failed"
            note: Optional[str] = None
            try:
                action = str(entry.get("action") or "").strip().lower()
                if action == "transfer":
                    status, note = self._execute_transfer(entry)
                else:
                    status, note = self._execute_adjustment(entry)
            except Exception as exc:
                status = "failed"
                note = f"execution error: {exc}"
                LOGGER.error("Adjustment %s execution failed: %s", request_id, exc)
            acked = self._acknowledge_adjustment(request_id, status, note)
            self._processed_adjustments[request_id] = {
                "status": status,
                "note": note,
                "acked": acked,
                "timestamp": time.time(),
            }
        self._prune_processed_adjustments()

    def _prune_processed_adjustments(self, ttl: float = 3600.0) -> None:
        if not self._processed_adjustments:
            return
        cutoff = time.time() - max(ttl, 60.0)
        for request_id, record in list(self._processed_adjustments.items()):
            if record.get("acked") and record.get("timestamp", 0) < cutoff:
                self._processed_adjustments.pop(request_id, None)

    def run_once(self) -> None:
        payload = self._collect()
        if payload is None:
            LOGGER.warning("Skipping coordinator update; unable to collect Paradex account data")
        else:
            self._push(payload)
            LOGGER.info(
                "Pushed Paradex monitor snapshot for %s (PnL %s)",
                self._label,
                payload["paradex_accounts"]["summary"].get("total_pnl"),
            )
        self._process_adjustments()

    def run_forever(self) -> None:
        LOGGER.info("Starting Paradex monitor for account %s", self._label)
        while True:
            start = time.time()
            try:
                self.run_once()
            except Exception as exc:  # pragma: no cover - continuous loop
                LOGGER.exception("Monitor iteration failed: %s", exc)
            elapsed = time.time() - start
            sleep_for = max(0.5, self._poll_interval - elapsed)
            time.sleep(sleep_for)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor a Paradex account and forward PnL data to the hedge coordinator",
    )
    parser.add_argument(
        "--coordinator-url",
        required=True,
        help="Hedge coordinator base URL, e.g. http://localhost:8899",
    )
    parser.add_argument("--coordinator-username", help="Optional Basic Auth username for coordinator access")
    parser.add_argument("--coordinator-password", help="Optional Basic Auth password for coordinator access")
    parser.add_argument("--agent-id", default="paradex-monitor", help="Agent identifier reported to the coordinator")
    parser.add_argument(
        "--account-label",
        default=os.getenv("PARADEX_ACCOUNT_LABEL", "default"),
        help="Label shown on the dashboard for this VPS/account",
    )
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_SECONDS, help="Seconds between refreshes")
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout for coordinator updates",
    )
    parser.add_argument(
        "--max-positions",
        type=int,
        default=MAX_ACCOUNT_POSITIONS,
        help="Maximum positions to include per account in the payload",
    )
    parser.add_argument(
        "--default-market",
        help="Fallback Paradex market (e.g. BTC-USD-PERP) when adjustments omit explicit symbols",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        help="Env file to preload (defaults to .env if present). Repeat to load multiple files.",
    )
    parser.add_argument("--once", action="store_true", help="Collect and push a single snapshot, then exit")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--log-file", help="Optional path to write agent logs (rotating file handler)")
    parser.add_argument(
        "--log-file-max-bytes",
        type=int,
        default=int(os.getenv("PARADEX_LOG_FILE_MAX_BYTES", "5242880")),
        help="Rotate log file after this many bytes (default 5 MiB)",
    )
    parser.add_argument(
        "--log-file-backup-count",
        type=int,
        default=int(os.getenv("PARADEX_LOG_FILE_BACKUP_COUNT", "5")),
        help="Number of rotated log files to keep",
    )
    parser.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable file logging even if defaults would enable it",
    )
    return parser.parse_args(argv)


def _resolve_log_file_path(args: argparse.Namespace) -> Optional[Path]:
    if getattr(args, "no_log_file", False):
        return None
    explicit = getattr(args, "log_file", None) or os.getenv("PARADEX_LOG_FILE")
    if explicit:
        return Path(explicit).expanduser()
    agent_label = args.agent_id or os.getenv("PARADEX_ACCOUNT_LABEL") or "paradex-monitor"
    safe_agent = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "-" for ch in agent_label)
    log_dir = Path(os.getenv("PARADEX_LOG_DIR", "logs")).expanduser()
    return log_dir / f"{safe_agent}.log"


def _configure_logging(args: argparse.Namespace) -> None:
    log_level_name = args.log_level
    level = getattr(logging, (log_level_name or "INFO").upper(), logging.INFO)
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers: List[logging.Handler] = [logging.StreamHandler()]

    log_file_path = _resolve_log_file_path(args)
    if log_file_path is not None:
        max_bytes = max(1024, int(args.log_file_max_bytes or 0))
        backup_count = max(1, int(args.log_file_backup_count or 1))
        try:
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            from logging.handlers import RotatingFileHandler

            file_handler = RotatingFileHandler(
                log_file_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
        except OSError as exc:  # pragma: no cover - filesystem guard
            print(f"Failed to set up log file {log_file_path}: {exc}", file=sys.stderr)

    logging.basicConfig(level=level, handlers=handlers, format=log_format)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    _configure_logging(args)

    env_files = args.env_file if args.env_file is not None else [".env"]
    load_env_files(env_files)

    try:
        creds = load_single_account(label=args.account_label)
    except ValueError as exc:
        LOGGER.error(str(exc))
        sys.exit(1)

    try:
        paradex_client = build_paradex_client(creds)
    except Exception as exc:
        LOGGER.error("Failed to initialise Paradex client: %s", exc)
        sys.exit(1)

    monitor = ParadexAccountMonitor(
        label=creds.label,
        paradex_client=paradex_client,
        coordinator_url=args.coordinator_url,
        agent_id=args.agent_id,
        poll_interval=args.poll_interval,
        request_timeout=args.request_timeout,
        max_positions=args.max_positions,
        coordinator_username=args.coordinator_username,
        coordinator_password=args.coordinator_password,
        default_market=args.default_market,
    )

    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
