#!/usr/bin/env python3
"""Paradex account monitor.

This helper polls the Paradex account using L2 credentials only, summarises PnL
and balance, and forwards the snapshot to the hedge coordinator so the dashboard
can display per-agent health.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import concurrent.futures
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Sequence, Tuple, cast

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

# When running this module as a standalone script (e.g. `python monitoring/para_account_monitor.py`)
# the repository root may not be on sys.path, so `helpers.*` can fail to import.
_PDT_ROOT = Path(__file__).resolve().parents[1]
if str(_PDT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PDT_ROOT))

try:
    from helpers.paradex_algo_client import ParadexAlgoClient, ParadexAlgoClientConfig
except ModuleNotFoundError:  # pragma: no cover
    # Fallback when helpers is not recognized as a package in some environments.
    from paradex_algo_client import ParadexAlgoClient, ParadexAlgoClientConfig  # type: ignore

try:
    from helpers.paradex_private_client import ParadexPrivateClient, ParadexPrivateClientConfig
except ModuleNotFoundError:  # pragma: no cover
    from paradex_private_client import ParadexPrivateClient, ParadexPrivateClientConfig  # type: ignore

try:  # pragma: no cover - optional dependency wiring
    from paradex_py import Paradex  # type: ignore
    from paradex_py.environment import PROD, TESTNET  # type: ignore
    from starknet_py.common import int_from_hex  # type: ignore
    from paradex_py.account.utils import flatten_signature  # type: ignore
    from paradex_py.message.order import build_order_message  # type: ignore
except ImportError:  # pragma: no cover - dependency guard
    Paradex = None  # type: ignore
    PROD = None  # type: ignore
    TESTNET = None  # type: ignore
    int_from_hex = None  # type: ignore
    flatten_signature = None  # type: ignore
    build_order_message = None  # type: ignore

getcontext().prec = 28

LOGGER = logging.getLogger("monitor.paradex_accounts")

# Enable extra, de-identified logging for IM requirement field debugging.
# NOTE: .env is loaded later in main(), so this must be evaluated at runtime.
def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_para_im_debug_enabled() -> bool:
    return _env_flag("PARA_IM_DEBUG")


def _is_para_twap_progress_debug_enabled() -> bool:
    return _env_flag("PARA_TWAP_PROGRESS_DEBUG")


def _is_para_twap_progress_history_only_enabled() -> bool:
    """When enabled, use GET /v1/algo/orders-history for the whole TWAP progress loop.

    Default is off because orders-history may be heavier and more ambiguous when multiple
    similar algos exist; but it is more reliable for capturing the final CLOSED state.
    """

    return _env_flag("PARA_TWAP_PROGRESS_HISTORY_ONLY")
DEFAULT_RPC_VERSION = "v0_9"
DEFAULT_POLL_SECONDS = 15.0
DEFAULT_TIMEOUT_SECONDS = 10.0

# TWAP progress polling defaults (used to refresh avg_price/filled_qty while algo is open).
DEFAULT_TWAP_PROGRESS_POLL_SECONDS = float(os.getenv("PARA_TWAP_PROGRESS_POLL_SECONDS", "2.0") or "2.0")
DEFAULT_TWAP_PROGRESS_MAX_SECONDS = float(os.getenv("PARA_TWAP_PROGRESS_MAX_SECONDS", "1800") or "1800")
# 每个账号上报的持仓条数上限。
# 之前默认 12 会导致 dashboard 看起来“最多只有 12 条持仓”。
# 设为 None 表示默认不截断（上报全量）。
MAX_ACCOUNT_POSITIONS: Optional[int] = None
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


def _normalize_rpc_version(value: Optional[str]) -> Optional[str]:
    """Normalize rpc_version formats (e.g. v0.8 / 0.8 / v0_8)."""
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    text = text.replace(".", "_").replace("-", "_")
    if not text.startswith("v"):
        text = f"v{text}"
    return text


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
    leverage_candidates = [
        ("leverage",),
        ("leverage_ratio",),
        ("leverageRatio",),
        ("leverage_ratio_cross",),
        ("leverage_ratio_isolated",),
        ("info", "leverage"),
        ("info", "leverage_ratio"),
        ("info", "leverageRatio"),
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
    raw_leverage = extract_from_paths(entry, *leverage_candidates)
    leverage_value = decimal_from(raw_leverage)

    # Keep payload mark_price as None when missing so downstream logic can replace it
    # with a fresher market summary. Use local fallbacks only for PnL calculation.
    calc_entry = entry_price
    calc_mark = mark_price
    if calc_entry is None and calc_mark is not None:
        calc_entry = calc_mark
    if calc_mark is None and calc_entry is not None:
        calc_mark = calc_entry

    if pnl_value is None and None not in (signed_size, calc_entry, calc_mark):
        try:
            pnl_value = (calc_mark - calc_entry) * signed_size  # type: ignore[arg-type]
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
        "leverage": decimal_to_str(leverage_value),
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

    PARADEX_RPC_VERSION env var (e.g. v0_8) is passed through to starknet fullnode RPC.
    If unset, defaults to v0_8 as recommended. This helps avoid "Invalid block id" errors when
    the node requires an explicit versioned RPC path.
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

    env_rpc_version = _normalize_rpc_version(os.getenv("PARADEX_RPC_VERSION"))
    rpc_version = env_rpc_version or DEFAULT_RPC_VERSION
    client = Paradex(env=env, logger=None)

    kwargs = {
        "l1_address": creds.l1_address,
        "l2_private_key": l2_private_key,
    }
    if rpc_version:
        sig = inspect.signature(client.init_account)
        if "rpc_version" in sig.parameters:
            kwargs["rpc_version"] = rpc_version
            LOGGER.info("Paradex init_account using rpc_version=%s", rpc_version)
        else:
            LOGGER.warning(
                "rpc_version=%s provided but init_account does not support rpc_version; "
                "upgrade paradex-py to enable versioned RPC (avoids Invalid block id)",
                rpc_version,
            )

    client.init_account(**kwargs)
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
        max_positions: Optional[int],
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
        # None 表示不截断（全量上报）；否则至少为 1。
        if max_positions in (None, 0):
            self._max_positions = None
        else:
            self._max_positions = max(1, int(max_positions))
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
        self._adjust_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
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
                LOGGER.debug("[PARA_IM_DEBUG] raw balance payload via %s: %s", name, payload)
                return payload
            if isinstance(payload, list) and payload:
                first = payload[0]
                if isinstance(first, dict):
                    LOGGER.debug("[PARA_IM_DEBUG] raw balance payload via %s (list[0]): %s", name, first)
                    return first
        return {}

    def _refresh_account_summary(self) -> Optional[Dict[str, Any]]:
        """Best-effort refresh of Paradex account summary.

        Paradex REST GET /v1/account returns fields like:
        - initial_margin_requirement
        - maintenance_margin_requirement
        - account_value
        - free_collateral

        Different paradex-py versions may expose different method names; we try a few.
        """

        api_client = getattr(self._client, "api_client", None)
        if api_client is None:
            return None

        # Authority (paradex-py): GET /v1/account == api_client.fetch_account_summary().
        # Only allow /v1/account/info fallback when explicitly enabled.
        allow_info_fallback = _env_flag("PARADEX_ALLOW_ACCOUNT_INFO_FALLBACK")

        candidate_methods = (
            "fetch_account_summary",
            "get_account_summary",
        )

        if allow_info_fallback:
            candidate_methods = candidate_methods + (
                "fetch_account_info",
                "get_account_info",
            )
        for name in candidate_methods:
            method = getattr(api_client, name, None)
            if method is None:
                continue
            try:
                payload = method()
            except TypeError:
                # Some methods expect a params dict.
                try:
                    payload = method({})
                except Exception as exc:  # pragma: no cover - network path
                    LOGGER.debug("Paradex %s call failed: %s", name, exc)
                    continue
            except Exception as exc:  # pragma: no cover - network path
                if _is_para_im_debug_enabled():
                    LOGGER.debug("[PARA_IM_DEBUG] Paradex %s call failed: %r", name, exc)
                else:
                    LOGGER.debug("Paradex %s call failed: %s", name, exc)
                continue

            # Some clients return coroutines.
            if inspect.iscoroutine(payload):
                try:
                    payload = asyncio.run(payload)  # best-effort in sync context
                except RuntimeError:
                    # If we're already in an event loop, skip awaiting.
                    payload = None
                except Exception as exc:  # pragma: no cover
                    LOGGER.debug("Paradex %s await failed: %s", name, exc)
                    payload = None

            # Normalise return type (paradex-py may return a dataclass for account summary).
            if payload is not None and not isinstance(payload, dict) and hasattr(payload, "__dict__"):
                try:
                    payload = dict(getattr(payload, "__dict__"))
                except Exception:
                    pass

            if isinstance(payload, dict):
                if _is_para_im_debug_enabled():
                    try:
                        LOGGER.debug(
                            "[PARA_IM_DEBUG] account_summary fetched via %s keys=%s",
                            name,
                            list(payload.keys()),
                        )
                        LOGGER.debug(
                            "[PARA_IM_DEBUG] account_summary via %s initial_margin_requirement=%s maintenance_margin_requirement=%s",
                            name,
                            payload.get("initial_margin_requirement"),
                            payload.get("maintenance_margin_requirement"),
                        )
                    except Exception:
                        pass
                return payload

        # Fallback (explicit): some SDKs expose api_client.account as a *property* (key material),
        # which is NOT the /v1/account summary. Only use this when explicitly enabled, otherwise
        # it misleads IM debugging.
        if allow_info_fallback:
            account_prop = getattr(api_client, "account", None)
            if account_prop is not None and not callable(account_prop):
                payload = getattr(account_prop, "__dict__", None)
                if isinstance(payload, dict) and payload:
                    if _is_para_im_debug_enabled():
                        LOGGER.debug(
                            "[PARA_IM_DEBUG] account_summary via api_client.account property keys=%s",
                            list(payload.keys()),
                        )
                    return payload
        return None

    @staticmethod
    def _extract_account_im_fields(account_obj: Any, account_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract IM requirement fields with REST field-name compatibility.

        Preferred:
        - account_obj.initial_margin_requirement (dashboard authoritative)

        REST /v1/account returns snake_case:
        - initial_margin_requirement

        Older/other SDKs might expose:
        - initial_margin_requirement
        - initial_margin
        """

        diag: Dict[str, Any] = {
            "initial_margin_requirement": None,
            "maintenance_margin_requirement": None,
            "initial_margin": None,
            "im_req_source": None,
            # Diagnostics
            "im_req_field": None,
        }

        def _pick_decimal(*values: Any) -> Optional[str]:
            for v in values:
                d = decimal_from(v)
                if d is not None:
                    return decimal_to_str(d)
            return None

        def _normalize_account_payload(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            """Unwrap common REST shapes.

            Some clients return:
              {"results": [{...account fields...}]}
            instead of a flat dict.
            """

            if not payload or not isinstance(payload, dict):
                return payload
            results = payload.get("results")
            if isinstance(results, list) and results:
                first = results[0]
                if isinstance(first, dict):
                    return first
            if isinstance(results, dict):
                return results
            return payload

        # 1) From SDK account object
        im_req = getattr(account_obj, "initial_margin_requirement", None)
        if im_req is None:
            # REST field name / older SDK mapping
            im_req = getattr(account_obj, "initial_margin_requirement", None)
        diag["initial_margin_requirement"] = _pick_decimal(im_req)
        if diag["initial_margin_requirement"] is not None:
            diag["im_req_source"] = "sdk.account"

        mm_req = getattr(account_obj, "maintenance_margin_requirement", None)
        if mm_req is None:
            mm_req = getattr(account_obj, "maintenance_margin_requirement", None)
        diag["maintenance_margin_requirement"] = _pick_decimal(mm_req)

        im_val = getattr(account_obj, "initial_margin", None)
        diag["initial_margin"] = _pick_decimal(im_val)

        # 2) From freshly fetched REST payload
        account_payload_norm = _normalize_account_payload(account_payload)
        if _is_para_im_debug_enabled() and isinstance(account_payload_norm, dict):
            try:
                # Only log structure + a few fields to avoid huge spam.
                sample = {k: account_payload_norm.get(k) for k in (
                    "initial_margin_requirement",
                    "maintenance_margin_requirement",
                    "initial_margin",
                    "account_value",
                    "free_collateral",
                    "equity",
                    "margin_requirement",
                ) if k in account_payload_norm or account_payload_norm.get(k) is not None}
                LOGGER.debug(
                    "[PARA_IM_DEBUG] normalized account_payload keys=%s sample=%s",
                    list(account_payload_norm.keys())[:50],
                    sample,
                )
            except Exception:
                pass
        if account_payload_norm and diag["initial_margin_requirement"] is None:
            # Try multiple possible keys used across API/SDK versions.
            candidates = (
                "initial_margin_requirement",
                "initialMarginRequirement",
                "initial_margin",
                "initialMargin",
                "im_requirement",
                "margin_requirement",
                "marginRequirement",
            )
            for key in candidates:
                if not isinstance(account_payload_norm, dict):
                    break
                if key not in account_payload_norm:
                    continue
                picked = _pick_decimal(account_payload_norm.get(key))
                if picked is None:
                    continue
                diag["initial_margin_requirement"] = picked
                diag["im_req_source"] = "rest:/v1/account"
                diag["im_req_field"] = key
                break

        if account_payload_norm and diag["maintenance_margin_requirement"] is None:
            mm_candidates = (
                "maintenance_margin_requirement",
                "maintenanceMarginRequirement",
                "maintenance_margin",
                "maintenanceMargin",
                "mm_requirement",
            )
            for key in mm_candidates:
                if not isinstance(account_payload_norm, dict):
                    break
                if key not in account_payload_norm:
                    continue
                picked = _pick_decimal(account_payload_norm.get(key))
                if picked is None:
                    continue
                diag["maintenance_margin_requirement"] = picked
                break

        return diag

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

        # Keep SDK account snapshot fresh. Some paradex-py versions only refresh account fields
        # after an explicit fetch call.
        account_snapshot = self._refresh_account_summary()
        positions = self._fetch_positions()

        if _is_para_im_debug_enabled():
            try:
                snapshot_keys = list(account_snapshot.keys()) if isinstance(account_snapshot, dict) else None
                LOGGER.debug(
                    "[PARA_IM_DEBUG] account_snapshot=%s keys=%s",
                    "ok" if isinstance(account_snapshot, dict) else "none",
                    snapshot_keys,
                )
                if isinstance(account_snapshot, dict):
                    im_raw = account_snapshot.get("initial_margin_requirement")
                    mm_raw = account_snapshot.get("maintenance_margin_requirement")
                    LOGGER.debug(
                        "[PARA_IM_DEBUG] account_snapshot fields initial_margin_requirement=%s maintenance_margin_requirement=%s",
                        im_raw,
                        mm_raw,
                    )
            except Exception as exc:  # pragma: no cover
                LOGGER.debug("[PARA_IM_DEBUG] snapshot logging failed: %s", exc)

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

        if self._max_positions is not None:
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
            # Dashboard-authoritative IM (preferred when available): Paradex account.initial_margin_requirement
            # We also ship `initial_margin` (if present) as a diagnostic/legacy field.
            "initial_margin_requirement": None,
            "maintenance_margin_requirement": None,
            "initial_margin": None,
            "im_req_source": None,
            "im_req_field": None,
            "updated_at": timestamp,
        }

        account_obj = getattr(self._client, "account", None)
        if account_obj is not None:
            try:
                extracted = self._extract_account_im_fields(account_obj, account_snapshot)
                summary["initial_margin_requirement"] = extracted.get("initial_margin_requirement")
                summary["maintenance_margin_requirement"] = extracted.get("maintenance_margin_requirement")
                summary["initial_margin"] = extracted.get("initial_margin")
                summary["im_req_source"] = extracted.get("im_req_source")
                summary["im_req_field"] = extracted.get("im_req_field")

                if _is_para_im_debug_enabled():
                    LOGGER.debug(
                        "[PARA_IM_DEBUG] extracted initial_margin_requirement=%s maintenance_margin_requirement=%s im_req_source=%s",
                        summary.get("initial_margin_requirement"),
                        summary.get("maintenance_margin_requirement"),
                        summary.get("im_req_source"),
                    )
            except Exception:
                pass

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
                        "initial_margin_requirement": summary.get("initial_margin_requirement"),
                        "maintenance_margin_requirement": summary.get("maintenance_margin_requirement"),
                        "initial_margin": summary.get("initial_margin"),
                        "im_req_source": summary.get("im_req_source"),
                        "im_req_field": summary.get("im_req_field"),
                        "positions": position_rows,
                        "updated_at": timestamp,
                    }
                ],
            },
        }

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

        transfer_kwargs: Dict[str, Any] = {}
        user_auto_estimate = payload.get("auto_estimate") if isinstance(payload, dict) else None
        resource_bounds = payload.get("resource_bounds") if isinstance(payload, dict) else None

        transfer_fn = getattr(self._client.account, "transfer_on_l2", None)
        supported_params: set[str] = set()
        if transfer_fn:
            try:
                supported_params = set(inspect.signature(transfer_fn).parameters)
            except (TypeError, ValueError):
                supported_params = set()

        if resource_bounds and "resource_bounds" in supported_params:
            transfer_kwargs["resource_bounds"] = resource_bounds
        if user_auto_estimate is not None and "auto_estimate" in supported_params:
            transfer_kwargs["auto_estimate"] = bool(user_auto_estimate)
        if "resource_bounds" not in transfer_kwargs and "auto_estimate" in supported_params:
            transfer_kwargs.setdefault("auto_estimate", True)

        # For older paradex-py where transfer_on_l2 does not accept auto_estimate/resource_bounds,
        # patch starknet.prepare_invoke to default auto_estimate=True when resource_bounds not provided.
        if "auto_estimate" not in supported_params and "resource_bounds" not in supported_params:
            starknet_obj = getattr(self._client.account, "starknet", None)
            prepare_fn = getattr(starknet_obj, "prepare_invoke", None)
            if starknet_obj and prepare_fn and not getattr(starknet_obj, "_auto_estimate_patched", False):
                original_prepare = prepare_fn

                async def _patched_prepare_invoke(calls, resource_bounds=None, auto_estimate=False, nonce=None):
                    if resource_bounds is None:
                        auto_estimate = True
                    return await original_prepare(calls, resource_bounds=resource_bounds, auto_estimate=auto_estimate, nonce=nonce)

                setattr(starknet_obj, "prepare_invoke", _patched_prepare_invoke)
                setattr(starknet_obj, "_auto_estimate_patched", True)

        try:
            transfer_coro = self._client.account.transfer_on_l2(target_address, amount, **transfer_kwargs)
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

    def _execute_adjustment(self, entry: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
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

        payload_cfg: Dict[str, Any] = cast(Dict[str, Any], entry.get("payload")) if isinstance(entry.get("payload"), dict) else {}
        order_mode = str(payload_cfg.get("order_mode") or "").strip().lower()
        twap_duration = payload_cfg.get("twap_duration_seconds")
        algo_type = str(payload_cfg.get("algo_type") or "").upper()

        # Default PARA adjustments to TWAP unless explicitly overridden.
        if not order_mode and not algo_type:
            order_mode = "twap"
            algo_type = "TWAP"

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

        twap_duration_seconds: Optional[int] = None
        if order_mode == "twap" or algo_type == "TWAP":
            try:
                duration_val = int(float(0 if twap_duration is None else twap_duration))
            except (TypeError, ValueError):
                duration_val = 900
            twap_duration_seconds = duration_val
            order = self._place_twap_order(symbol, side, trade_quantity, duration_val)
            note_suffix_parts = [note_suffix] if note_suffix else []
            note_suffix_parts.append(f"TWAP {duration_val}s")
            note_suffix = "; ".join([part for part in note_suffix_parts if part]) or None
        else:
            order = self._place_market_order(symbol, side, trade_quantity)
        order_id = None
        filled_qty: Any = None
        avg_price: Any = None
        if isinstance(order, dict):
            order_id = order.get("id") or order.get("order_id")
            # Response shape depends on endpoint:
            # - POST /orders (market) can resemble a fill (size/price)
            # - POST /algo/orders (TWAP) returns an algo order (size is total size, NOT filled)
            if order_mode == "twap" or algo_type == "TWAP":
                # For TWAP we rely on progress polling (GET /algo/orders) to stream filled_qty/avg_price.
                filled_qty = order.get("filled_size")
            else:
                # Market order: best-effort filled size from response.
                filled_qty = order.get("filled_size")
                if filled_qty is None:
                    filled_qty = (
                        order.get("size")  # FillResult.size
                        or order.get("filled")
                        or order.get("filled_qty")
                        or order.get("filled_quantity")
                    )
            avg_price = order.get("avg_price")
            if avg_price is None:
                avg_price = (
                    order.get("avg_fill_price")  # AlgoOrderResp.avg_fill_price
                    or order.get("avgFillPrice")
                    or order.get("average_price")
                    or order.get("price")  # FillResult.price (if only a single fill/market fill)
                )
        delta = trade_quantity if side == "buy" else -trade_quantity
        new_net = net_size + delta
        self._update_cached_position(symbol, new_net)

        mode_label = "TWAP" if (order_mode == "twap" or algo_type == "TWAP") else "MARKET"
        fill_note_parts: List[str] = []
        if filled_qty is not None:
            fill_note_parts.append(f"filled={filled_qty}")
        if avg_price is not None:
            fill_note_parts.append(f"avg_price={avg_price}")
        fill_note = "; ".join(fill_note_parts)

        note_core = (
            f"{side.upper()} {decimal_to_str(trade_quantity) or trade_quantity} {symbol} "
            f"(net {decimal_to_str(net_size) or net_size} -> {decimal_to_str(new_net) or new_net}); "
            f"order_type={mode_label}"
        )
        if note_suffix:
            note_core = f"{note_core}; {note_suffix}"
        if fill_note:
            note_core = f"{note_core}; {fill_note}"

        ack_extra: Dict[str, Any] = {
            "order_type": mode_label,
        }
        # NOTE: These keys are internal to perp-dex-tools and are populated from Paradex API responses.
        # When changing PARA API integration, please reference paradex-py (generated schemas):
        # - AlgoOrderResp.avg_fill_price -> here we publish as avg_price
        # - FillResult.size -> here we publish as filled_qty
        if order_id is not None:
            ack_extra["order_id"] = order_id
        if filled_qty is not None:
            ack_extra["filled_qty"] = str(filled_qty)
        if avg_price is not None:
            ack_extra["avg_price"] = str(avg_price)

        # Carry enough context for streaming TWAP progress updates.
        # This is best-effort and allows the dashboard history to refresh avg_price/filled_qty
        # while the algo order is still open.
        if mode_label == "TWAP":
            ack_extra["algo_market"] = symbol
            ack_extra["algo_side"] = side.upper()
            ack_extra["algo_expected_size"] = str(trade_quantity)
            if twap_duration_seconds is not None:
                ack_extra["twap_duration_seconds"] = int(twap_duration_seconds)
            ack_extra["algo_started_at_ms"] = int(time.time() * 1000)

        note_out = note_core if order_id is None else f"{note_core}; order_id={order_id}"
        return "succeeded", note_out, ack_extra

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

    def _place_twap_order(self, symbol: str, side: str, quantity: Decimal, duration_seconds: int) -> Any:
        """Place a TWAP order via algo/orders when signing utilities are available."""
        if flatten_signature is None or build_order_message is None:
            raise RuntimeError("TWAP requires paradex-py signing helpers (flatten_signature/build_order_message)")
        if quantity <= 0:
            raise ValueError("Order quantity must be positive")
        if not self._client or not getattr(self._client, "account", None):
            raise RuntimeError("Paradex account client not initialized")
        from paradex_py.common.order import Order, OrderType, OrderSide  # type: ignore

        order_side = OrderSide.Buy if side.lower() == "buy" else OrderSide.Sell
        # Paradex TWAP API validation expects:
        # - algo_type: non-blank (e.g. "TWAP")
        # - duration_seconds >= freq (freq minimum is 30 seconds)
        # We snap duration to 30s grid to keep behavior stable.
        freq_seconds = 30
        duration = max(
            freq_seconds,
            min(86400, int(round(int(duration_seconds) / freq_seconds) * freq_seconds)),
        )
        order = Order(
            market=symbol,
            order_type=OrderType.Market,
            order_side=order_side,
            size=quantity,
            signature_timestamp=int(time.time() * 1000),
        )
        message = build_order_message(self._client.account.l2_chain_id, order)
        signature = self._sign_paradex_message(message)
        signature = flatten_signature(signature)
        valid_until = int(time.time() * 1000) + duration * 1000
        payload = {
            "algo_type": "TWAP",
            "market": symbol,
            "side": order_side.name.upper(),
            "size": str(order.size),
            "type": "MARKET",
            "freq": freq_seconds,
            "duration_seconds": duration,
            "signature": signature,
            "signature_timestamp": order.signature_timestamp,
            "valid_until": valid_until,
        }
        return self._client.api_client._post_authorized(path="algo/orders", payload=payload)

    def _sign_paradex_message(self, message: Any) -> Any:
        """Sign an L2 order message.

        方案A（当前首选）：不要依赖 `ParadexAccount.sign_*`。
        paradex-py 源码里的 `ParadexAccount` 内部通常会持有一个 starknet 账户对象
        `account.starknet`，它提供 `sign_message(typed_data)`。

        兼容兜底：有些版本会在 api_client 上注入 `signer`（protocols.Signer），但
        它通常是给 REST /orders 用的（签 order dict），这里仍尽量尝试。
        """

        if not self._client or not getattr(self._client, "account", None):
            raise RuntimeError("Paradex account client not initialized")
        acct = self._client.account

        # Preferred: use underlying starknet account held by ParadexAccount.
        starknet_acct = getattr(acct, "starknet", None)
        fn = getattr(starknet_acct, "sign_message", None)
        if callable(fn):
            return fn(message)

        # Alternative: some setups may expose a signer on the api client.
        # Note: this signer typically signs order dict payloads, not typed data.
        api_client = getattr(self._client, "api_client", None)
        signer = getattr(api_client, "signer", None) if api_client is not None else None
        fn = getattr(signer, "sign_message", None)
        if callable(fn):
            return fn(message)

        # Legacy fallbacks (kept for older installs), but no longer required for TWAP.
        for attr in ("sign_message", "sign", "sign_message_hash", "sign_hash"):
            fn = getattr(acct, attr, None)
            if callable(fn):
                return fn(message)

        raise AttributeError(
            "No Paradex signing capability found for TWAP. Tried: account.starknet.sign_message, "
            "api_client.signer.sign_message, and account sign_* fallbacks. "
            "This usually means the Paradex client wasn't initialized with keys or uses an incompatible SDK build."
        )

    def _acknowledge_adjustment(
        self,
        request_id: str,
        status: str,
        note: Optional[str],
        extra: Optional[Dict[str, Any]] = None,
    ) -> bool:
        payload = {
            "request_id": request_id,
            "agent_id": self._agent_id,
            "status": status,
        }
        if note is not None:
            payload["note"] = note
        if isinstance(extra, dict) and extra:
            payload.update(extra)
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

    def _poll_twap_progress(self, request_id: str) -> None:
        """Poll open algo orders and stream TWAP progress updates.

        Uses Paradex REST GET /v1/algo/orders (via perp-dex-tools helper client)
        and updates coordinator with progress=true payloads.

        The monitor will stop polling when the matching TWAP is no longer listed as open,
        or when a time limit is reached.
        """

        debug_enabled = _is_para_twap_progress_debug_enabled()
        history_only = _is_para_twap_progress_history_only_enabled()

        record = self._processed_adjustments.get(request_id) or {}
        extra = record.get("extra") if isinstance(record.get("extra"), dict) else {}
        if not isinstance(extra, dict):
            if debug_enabled:
                LOGGER.info("TWAP progress[%s] skipped: missing extra dict", request_id)
            return

        market = str(extra.get("algo_market") or "").strip()
        side = str(extra.get("algo_side") or "").strip().upper()
        expected_size = str(extra.get("algo_expected_size") or "").strip()
        started_at_ms = int(extra.get("algo_started_at_ms") or 0)
        if not market or side not in {"BUY", "SELL"} or not expected_size:
            if debug_enabled:
                LOGGER.info(
                    "TWAP progress[%s] skipped: match context incomplete market=%s side=%s expected_size=%s",
                    request_id,
                    market or "",
                    side or "",
                    expected_size or "",
                )
            return

        duration_hint = extra.get("twap_duration_seconds")
        try:
            duration_hint_val = int(duration_hint) if duration_hint is not None else None
        except Exception:
            duration_hint_val = None

        poll_interval = max(0.5, DEFAULT_TWAP_PROGRESS_POLL_SECONDS)
        hard_deadline = time.time() + max(30.0, DEFAULT_TWAP_PROGRESS_MAX_SECONDS)
        if duration_hint_val is not None and duration_hint_val > 0:
            hard_deadline = min(hard_deadline, time.time() + float(duration_hint_val) + 120.0)

        def _to_float(value: Any) -> Optional[float]:
            if value is None:
                return None
            try:
                return float(str(value))
            except Exception:
                return None

        def _match_algo(row: Dict[str, Any]) -> bool:
            if not isinstance(row, dict):
                return False
            if str(row.get("algo_type") or "").upper() != "TWAP":
                return False
            if str(row.get("market") or "").strip().upper() != market.upper():
                return False
            if str(row.get("side") or "").strip().upper() != side:
                return False
            if str(row.get("size") or "").strip() != expected_size:
                return False
            if started_at_ms:
                try:
                    created_at_ms = int(row.get("created_at") or 0)
                except Exception:
                    created_at_ms = 0
                if created_at_ms and abs(created_at_ms - started_at_ms) > 120_000:
                    return False
            return True

        last_sent: Dict[str, Any] = {}

        # Build local algo client config from the existing Paradex client.
        api_client = getattr(self._client, "api_client", None) if self._client is not None else None
        base_url = getattr(api_client, "api_url", None)
        token: Optional[str] = None

        def _extract_bearer(value: Any) -> Optional[str]:
            if not isinstance(value, str):
                return None
            text = value.strip()
            if not text.lower().startswith("bearer "):
                return None
            out = text.split(" ", 1)[1].strip()
            return out or None

        # 1) Preferred: read Authorization header from underlying http client.
        try:
            headers = getattr(getattr(api_client, "client", None), "headers", None)
            auth_val: Any = None

            # httpx.Client.headers is a `Headers` object (dict-like but not a dict).
            if headers is not None:
                getter = getattr(headers, "get", None)
                if callable(getter):
                    auth_val = getter("Authorization")
                if auth_val is None:
                    # Some stacks normalize to lower-case.
                    auth_val = getter("authorization") if callable(getter) else None

            token = _extract_bearer(auth_val)
        except Exception:
            token = None

        # 2) Fallbacks: paradex-py may store token separately.
        if not token and api_client is not None:
            token = getattr(api_client, "_manual_token", None)
        if not token and self._client is not None:
            acct = getattr(self._client, "account", None)
            token = getattr(acct, "jwt_token", None) if acct is not None else None

        algo_client: Optional[ParadexAlgoClient] = None
        private_client: Optional[ParadexPrivateClient] = None
        if isinstance(base_url, str) and base_url and isinstance(token, str) and token:
            base_url_norm = base_url.rstrip("/")
            timeout_val = float(getattr(self, "_timeout", DEFAULT_TIMEOUT_SECONDS) or DEFAULT_TIMEOUT_SECONDS)
            algo_client = ParadexAlgoClient(
                ParadexAlgoClientConfig(
                    base_url=base_url_norm,
                    jwt_token=token,
                    timeout_seconds=timeout_val,
                )
            )
            private_client = ParadexPrivateClient(
                ParadexPrivateClientConfig(
                    base_url=base_url_norm,
                    jwt_token=token,
                    timeout_seconds=timeout_val,
                )
            )

        if debug_enabled:
            LOGGER.info(
                "TWAP progress[%s] start poll: market=%s side=%s expected_size=%s poll=%.1fs deadline=%.0fs base_url=%s token=%s",
                request_id,
                market,
                side,
                expected_size,
                poll_interval,
                hard_deadline - time.time(),
                str(base_url or "")[:64],
                "ok" if algo_client is not None else "missing",
            )

        def _extract_rows(resp_obj: Any) -> List[Dict[str, Any]]:
            if not isinstance(resp_obj, dict):
                return []
            # Some API wrappers may nest data under "data" or "result".
            candidates: List[Any] = []
            candidates.append(resp_obj.get("results"))
            candidates.append(resp_obj.get("data"))
            candidates.append(resp_obj.get("result"))
            data = resp_obj.get("data")
            if isinstance(data, dict):
                candidates.append(data.get("results"))
            result = resp_obj.get("result")
            if isinstance(result, dict):
                candidates.append(result.get("results"))

            for candidate in candidates:
                if isinstance(candidate, list):
                    return [r for r in candidate if isinstance(r, dict)]
            return []

        def _debug_dump_history(resp_obj: Any) -> None:
            if not debug_enabled:
                return
            if not isinstance(resp_obj, dict):
                LOGGER.info("TWAP progress[%s] history raw type=%s", request_id, type(resp_obj).__name__)
                return
            keys = sorted([str(k) for k in resp_obj.keys()])
            rows = _extract_rows(resp_obj)
            LOGGER.info(
                "TWAP progress[%s] history raw keys=%s results=%s",
                request_id,
                keys,
                len(rows),
            )
            for idx, row in enumerate(rows[:3]):
                LOGGER.info(
                    "TWAP progress[%s] history sample[%s]: id=%s market=%s side=%s type=%s status=%s size=%s remaining=%s avg=%s created_at=%s updated_at=%s",
                    request_id,
                    idx,
                    row.get("id"),
                    row.get("market"),
                    row.get("side"),
                    row.get("algo_type"),
                    row.get("status"),
                    row.get("size"),
                    row.get("remaining_size"),
                    row.get("avg_fill_price"),
                    row.get("created_at"),
                    row.get("last_updated_at"),
                )

        def _status_is_openish(status_val: Any) -> bool:
            text = str(status_val or "").strip().upper()
            return text in {"OPEN", "NEW"}

        # Unit-test friendliness: when timeout is configured extremely small, avoid long sleeps.
        loop_guard = 0

        while time.time() < hard_deadline:
            loop_guard += 1
            if getattr(self, "_timeout", DEFAULT_TIMEOUT_SECONDS) is not None:
                try:
                    if float(getattr(self, "_timeout", DEFAULT_TIMEOUT_SECONDS)) < 0.01 and loop_guard > 2:
                        return
                except Exception:
                    pass
            try:
                if private_client is None:
                    # Older setups may not expose base_url/token; in that case we can't poll.
                    if debug_enabled:
                        LOGGER.info("TWAP progress[%s] stop: missing base_url/token for algo client", request_id)
                    return
                if history_only:
                    # Use history endpoint for the full lifecycle (OPEN/NEW/CLOSED).
                    window_start = started_at_ms - 10 * 60_000 if started_at_ms else None
                    window_end = started_at_ms + 60 * 60_000 if started_at_ms else None
                    resp = private_client.fetch_algo_orders_history(
                        status=None,
                        market=market,
                        side=side,
                        algo_type="TWAP",
                        start=window_start,
                        end=window_end,
                        limit=50,
                    )
                    _debug_dump_history(resp)
                else:
                    if algo_client is None:
                        if debug_enabled:
                            LOGGER.info("TWAP progress[%s] stop: missing base_url/token for algo client", request_id)
                        return
                    resp = algo_client.fetch_open_algo_orders()
            except Exception as exc:  # pragma: no cover
                LOGGER.debug("TWAP progress algo/orders fetch failed for %s: %s", request_id, exc)
                time.sleep(poll_interval)
                continue

            rows = _extract_rows(resp)

            if debug_enabled:
                LOGGER.info(
                    "TWAP progress[%s] fetched %s algos (%s)",
                    request_id,
                    len(rows),
                    "history" if history_only else "open",
                )

            algo = next((row for row in rows if _match_algo(row)), None)
            if not algo:
                if debug_enabled:
                    LOGGER.info("TWAP progress[%s] stop: no matching TWAP in open orders", request_id)
                # If the TWAP is already CLOSED, it may disappear from /algo/orders quickly.
                # Use /algo/orders-history to try to capture the last fill (avg_fill_price / remaining_size).
                if private_client is not None and not history_only:
                    try:
                        # Narrow window around start time to avoid picking wrong algo when multiple exist.
                        window_start = started_at_ms - 10 * 60_000 if started_at_ms else None
                        window_end = started_at_ms + 60 * 60_000 if started_at_ms else None
                        history = private_client.fetch_algo_orders_history(
                            status="CLOSED",
                            market=market,
                            side=side,
                            algo_type="TWAP",
                            start=window_start,
                            end=window_end,
                            limit=50,
                        )
                        hist_rows_raw = history.get("results") if isinstance(history, dict) else None
                        hist_rows: List[Dict[str, Any]] = []
                        if isinstance(hist_rows_raw, list):
                            hist_rows = [r for r in hist_rows_raw if isinstance(r, dict)]

                        # Match by same criteria but loosen created_at window slightly.
                        def _match_hist(row: Dict[str, Any]) -> bool:
                            if not isinstance(row, dict):
                                return False
                            if str(row.get("algo_type") or "").upper() != "TWAP":
                                return False
                            if str(row.get("market") or "").strip().upper() != market.upper():
                                return False
                            if str(row.get("side") or "").strip().upper() != side:
                                return False
                            if str(row.get("size") or "").strip() != expected_size:
                                return False
                            if started_at_ms:
                                try:
                                    created_at_ms = int(row.get("created_at") or 0)
                                except Exception:
                                    created_at_ms = 0
                                if created_at_ms and abs(created_at_ms - started_at_ms) > 30 * 60_000:
                                    return False
                            return True

                        hist_algo = next((row for row in hist_rows if _match_hist(row)), None)
                        if hist_algo is not None:
                            size_val = _to_float(hist_algo.get("size"))
                            remaining_val = _to_float(hist_algo.get("remaining_size"))
                            filled_val: Optional[float] = None
                            if size_val is not None and remaining_val is not None:
                                filled_val = max(0.0, size_val - remaining_val)

                            progress_extra: Dict[str, Any] = {
                                "order_type": "TWAP",
                                "algo_id": hist_algo.get("id"),
                                "algo_status": hist_algo.get("status"),
                                "algo_last_updated_at": hist_algo.get("last_updated_at"),
                                "algo_remaining_size": hist_algo.get("remaining_size"),
                                "algo_size": hist_algo.get("size"),
                            }
                            if hist_algo.get("avg_fill_price") is not None:
                                progress_extra["avg_price"] = str(hist_algo.get("avg_fill_price"))
                            if filled_val is not None:
                                progress_extra["filled_qty"] = str(filled_val)

                            if progress_extra != last_sent:
                                ok = self._acknowledge_adjustment(
                                    request_id,
                                    "pending",
                                    None,
                                    {"progress": True, **progress_extra},
                                )
                                if debug_enabled:
                                    LOGGER.info(
                                        "TWAP progress[%s] history ack progress=%s status=%s avg_price=%s filled_qty=%s",
                                        request_id,
                                        "ok" if ok else "fail",
                                        progress_extra.get("algo_status"),
                                        progress_extra.get("avg_price"),
                                        progress_extra.get("filled_qty"),
                                    )
                                if ok:
                                    merged = dict(extra)
                                    merged.update(progress_extra)
                                    record["extra"] = merged
                                    record["timestamp"] = time.time()
                                    self._processed_adjustments[request_id] = record
                                    last_sent = dict(progress_extra)
                    except Exception as exc:  # pragma: no cover
                        LOGGER.debug("TWAP progress[%s] history fallback failed: %s", request_id, exc)
                break  # no longer open

            # In history-only mode, stop only after it is no longer OPEN/NEW.
            if history_only and not _status_is_openish(algo.get("status")):
                # send one last update below (progress_extra) and then break.
                pass

            size_val = _to_float(algo.get("size"))
            remaining_val = _to_float(algo.get("remaining_size"))
            filled_val: Optional[float] = None
            if size_val is not None and remaining_val is not None:
                filled_val = max(0.0, size_val - remaining_val)

            progress_extra: Dict[str, Any] = {
                "order_type": "TWAP",
                "algo_id": algo.get("id"),
                "algo_status": algo.get("status"),
                "algo_last_updated_at": algo.get("last_updated_at"),
                "algo_remaining_size": algo.get("remaining_size"),
                "algo_size": algo.get("size"),
            }
            if algo.get("avg_fill_price") is not None:
                progress_extra["avg_price"] = str(algo.get("avg_fill_price"))
            if filled_val is not None:
                progress_extra["filled_qty"] = str(filled_val)

            if progress_extra != last_sent:
                ok = self._acknowledge_adjustment(
                    request_id,
                    "pending",
                    None,
                    {"progress": True, **progress_extra},
                )
                if debug_enabled:
                    LOGGER.info(
                        "TWAP progress[%s] ack progress=%s status=%s avg_price=%s filled_qty=%s remaining=%s",
                        request_id,
                        "ok" if ok else "fail",
                        progress_extra.get("algo_status"),
                        progress_extra.get("avg_price"),
                        progress_extra.get("filled_qty"),
                        progress_extra.get("algo_remaining_size"),
                    )
                if ok:
                    merged = dict(extra)
                    merged.update(progress_extra)
                    record["extra"] = merged
                    record["timestamp"] = time.time()
                    self._processed_adjustments[request_id] = record
                    last_sent = dict(progress_extra)

            if history_only and not _status_is_openish(progress_extra.get("algo_status")):
                if debug_enabled:
                    LOGGER.info(
                        "TWAP progress[%s] stop: history status=%s", request_id, progress_extra.get("algo_status")
                    )
                break

            if getattr(self, "_timeout", DEFAULT_TIMEOUT_SECONDS) is not None:
                try:
                    if float(getattr(self, "_timeout", DEFAULT_TIMEOUT_SECONDS)) < 0.01:
                        return
                except Exception:
                    pass

            time.sleep(poll_interval)

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
            if cache and cache.get("acked"):
                continue
            if cache and cache.get("inflight"):
                continue
            def _worker(entry_copy: Dict[str, Any], req_id: str) -> None:
                status = "failed"
                note: Optional[str] = None
                extra: Optional[Dict[str, Any]] = None
                start_progress = False
                try:
                    action = str(entry_copy.get("action") or "").strip().lower()
                    if action == "transfer":
                        status, note = self._execute_transfer(entry_copy)
                    else:
                        status, note, extra = self._execute_adjustment(entry_copy)
                        if isinstance(extra, dict) and extra.get("order_type") == "TWAP":
                            start_progress = True
                except Exception as exc:  # pragma: no cover - runtime error path
                    status = "failed"
                    note = f"execution error: {exc}"
                    LOGGER.error("Adjustment %s execution failed: %s", req_id, exc)
                acked = self._acknowledge_adjustment(req_id, status, note, extra)
                self._processed_adjustments[req_id] = {
                    "status": status,
                    "note": note,
                    "extra": extra,
                    "acked": acked,
                    "timestamp": time.time(),
                    "inflight": False,
                }

                if acked and start_progress:
                    record = self._processed_adjustments.get(req_id) or {}
                    if not record.get("progress_polling"):
                        record["progress_polling"] = True
                        self._processed_adjustments[req_id] = record
                        try:
                            self._adjust_executor.submit(self._poll_twap_progress, req_id)
                        except Exception as exc:  # pragma: no cover
                            LOGGER.debug("Failed to start TWAP progress poller for %s: %s", req_id, exc)

            self._processed_adjustments[request_id] = {
                "status": "pending",
                "note": None,
                "acked": False,
                "timestamp": time.time(),
                "inflight": True,
                "progress_polling": False,
            }
            try:
                self._adjust_executor.submit(_worker, dict(entry), request_id)
            except Exception as exc:  # pragma: no cover - executor failure
                LOGGER.error("Failed to submit adjustment %s: %s", request_id, exc)
                self._processed_adjustments[request_id] = {
                    "status": "failed",
                    "note": f"submit failed: {exc}",
                    "acked": False,
                    "timestamp": time.time(),
                    "inflight": False,
                }
        self._prune_processed_adjustments()

    def _prune_processed_adjustments(self, ttl: float = 3600.0) -> None:
        if not self._processed_adjustments:
            return
        cutoff = time.time() - max(ttl, 60.0)
        for request_id, record in list(self._processed_adjustments.items()):
            if (
                record.get("acked")
                and not record.get("progress_polling")
                and record.get("timestamp", 0) < cutoff
            ):
                self._processed_adjustments.pop(request_id, None)

    def run_once(self) -> None:
        payload = self._collect()
        if payload is None or not isinstance(payload, dict):
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
        default=0 if MAX_ACCOUNT_POSITIONS is None else int(MAX_ACCOUNT_POSITIONS),
        help="Maximum positions to include per account in the payload (0 = all)",
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
    parser.add_argument(
        "--quiet-http",
        action="store_true",
        help="Suppress noisy DEBUG logs from http libraries (httpcore/httpx/urllib3).",
    )
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

    # NOTE: PARADEX_QUIET_HTTP from .env is loaded later; we re-apply after load_env_files().
    if bool(getattr(args, "quiet_http", False)):
        _apply_quiet_http_logging()


def _apply_quiet_http_logging() -> None:
    noisy_loggers = (
        "httpcore",
        "httpx",
        "urllib3",
        "hpack",
        "h2",
        "charset_normalizer",
    )
    for name in noisy_loggers:
        logging.getLogger(name).setLevel(logging.WARNING)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    _configure_logging(args)

    env_files = args.env_file if args.env_file is not None else [".env"]
    load_env_files(env_files)

    # Re-apply env-controlled toggles after .env is loaded.
    quiet_http = bool(getattr(args, "quiet_http", False) or _env_flag("PARADEX_QUIET_HTTP"))
    if quiet_http:
        _apply_quiet_http_logging()

    if _is_para_im_debug_enabled() or quiet_http:
        LOGGER.info(
            "Diagnostics: PARA_IM_DEBUG=%s, PARADEX_QUIET_HTTP=%s (env_files=%s)",
            "on" if _is_para_im_debug_enabled() else "off",
            "on" if quiet_http else "off",
            ",".join(env_files),
        )

    if _is_para_im_debug_enabled() and _env_flag("PARADEX_ALLOW_ACCOUNT_INFO_FALLBACK"):
        LOGGER.info("Diagnostics: PARADEX_ALLOW_ACCOUNT_INFO_FALLBACK=on")

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
