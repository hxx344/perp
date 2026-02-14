#!/usr/bin/env python3
"""GRVT account monitor.

Each VPS runs this helper for *its* GRVT account. The script polls positions,
summarises PnL, and forwards everything to the hedge coordinator so the
dashboard can show per-VPS health without juggling multiple account flags.
"""

from __future__ import annotations

import argparse
import importlib
import json
import base64
import logging
import os
import random
import re
import sys
import time
import threading
from collections import deque
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, cast
from logging.handlers import RotatingFileHandler

import requests
try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    websocket = None  # type: ignore
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

try:  # pragma: no cover - optional dependency wiring
    from eth_account import Account as EthAccount
except ImportError:  # pragma: no cover - fallback when pysdk deps missing
    EthAccount = None  # type: ignore

try:  # pragma: no cover - optional dependency wiring
    from web3 import Web3
except ImportError:  # pragma: no cover - allow running without web3
    Web3 = None  # type: ignore

try:  # pragma: no cover - optional dependency wiring
    from pysdk.grvt_fixed_types import Transfer as GrvtTransfer
    from pysdk.grvt_raw_base import GrvtApiConfig, GrvtError
    from pysdk.grvt_raw_env import GrvtEnv as RawGrvtEnv
    from pysdk.grvt_raw_signing import sign_transfer
    from pysdk.grvt_raw_sync import GrvtRawSync
    from pysdk.grvt_raw_types import ApiTransferRequest, Signature as GrvtSignature, TransferType
    from pysdk.grvt_ccxt_env import GrvtEndpointType, get_grvt_endpoint_domains
    from pysdk.grvt_ccxt_utils import GrvtCurrency
except ImportError:  # pragma: no cover - fallback guard
    GrvtTransfer = None  # type: ignore
    GrvtApiConfig = None  # type: ignore
    RawGrvtEnv = None  # type: ignore
    sign_transfer = None  # type: ignore
    GrvtRawSync = None  # type: ignore
    ApiTransferRequest = None  # type: ignore
    GrvtSignature = None  # type: ignore
    TransferType = None  # type: ignore
    GrvtEndpointType = None  # type: ignore
    get_grvt_endpoint_domains = None  # type: ignore
    GrvtCurrency = None  # type: ignore

getcontext().prec = 28

LOGGER = logging.getLogger("monitor.grvt_accounts")
DEFAULT_POLL_SECONDS = 15.0
DEFAULT_TIMEOUT_SECONDS = 10.0
# 每个账号上报的持仓条数上限。默认 0 表示不截断（全量上报）。
MAX_ACCOUNT_POSITIONS = 0
VOLATILITY_SAMPLE_INTERVAL = 10.0
VOLATILITY_WINDOWS: Dict[str, float] = {
    "10m": 10 * 60.0,
    "60m": 60 * 60.0,
    "300m": 300 * 60.0,
}
VOLATILITY_TOPN = 3
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
)
DEFAULT_TRANSFER_CURRENCY = "USDT"
DEFAULT_TRANSFER_CURRENCY_ID = 3
SIGNATURE_TTL_SECONDS = 15 * 60
TRANSFER_TYPE_ALIASES: Dict[str, str] = {
    "INTERNAL": "STANDARD",
    "SPOT": "STANDARD",
    "SPOT_TRANSFER": "STANDARD",
    "SPOT_INTERNAL": "STANDARD",
    "DEFAULT": "STANDARD",
}
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
        loaded_count = 0
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
                        loaded_count += 1
        except Exception as exc:
            LOGGER.warning("Failed to load env file %s: %s", env_path, exc)
            continue
        LOGGER.info("Loaded %s variables from %s", loaded_count, env_path)



@dataclass(frozen=True)
class AccountCredentials:
    label: str
    trading_account_id: str
    private_key: str
    api_key: str
    environment: str
    main_account_id: Optional[str] = None
    main_sub_account_id: str = "0"


@dataclass
class AccountSession:
    label: str
    client: Any
    main_account_id: Optional[str] = None
    sub_account_id: Optional[str] = None
    main_sub_account_id: str = "0"


def import_grvt_sdk():  # pragma: no cover - thin import wrapper
    try:
        grvt_ccxt = importlib.import_module("pysdk.grvt_ccxt")
        grvt_env_module = importlib.import_module("pysdk.grvt_ccxt_env")
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ImportError(
            "grvt_account_monitor requires grvt-pysdk. Install it via 'pip install grvt-pysdk'."
        ) from exc
    return grvt_ccxt.GrvtCcxt, grvt_env_module.GrvtEnv


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


def guess_symbol(entry: Dict[str, Any]) -> str:
    fields = [
        entry.get("symbol"),
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

    # Normalise common delimiters so we can reliably split the base leg
    for token in (":", "-", "_", " "):
        text = text.replace(token, "/")
    parts = [part for part in text.split("/") if part]
    candidate = parts[0] if parts else text

    # Strip common suffixes so ETHPERP / BTCUSDT -> ETH / BTC
    suffixes = ("PERP", "FUT", "FUTURES", "USD", "USDT", "USDC")
    stripped = True
    while stripped and candidate:
        stripped = False
        for suffix in suffixes:
            if candidate.endswith(suffix) and len(candidate) > len(suffix):
                candidate = candidate[: -len(suffix)]
                stripped = True
                break

    match = re.match(r"[A-Z]+", candidate)
    if match:
        candidate = match.group(0)

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
        ("quantity",),
        ("info", "position_size"),
        ("info", "contracts"),
    ]
    entry_price_candidates = [
        ("entry_price",),
        ("entryPrice",),
        ("average_price",),
        ("avg_entry_price",),
        ("info", "entry_price"),
        ("info", "average_entry_price"),
    ]
    mark_price_candidates = [
        ("mark_price",),
        ("markPrice",),
        ("last_price",),
        ("info", "mark_price"),
        ("info", "markPrice"),
        ("info", "last_price"),
    ]
    pnl_candidates = [
        ("unrealizedPnl",),
        ("unrealized_pnl",),
        ("pnl",),
        ("info", "unrealizedPnl"),
        ("info", "unrealized_pnl"),
        ("info", "pnl"),
    ]
    isolated_im_candidates = [
        ("isolated_im",),
        ("isolatedIM",),
        ("initial_margin",),
        ("initialMargin",),
        ("info", "isolated_im"),
        ("info", "isolatedIM"),
        ("info", "initial_margin"),
        ("info", "initialMargin"),
    ]
    isolated_mm_candidates = [
        ("isolated_mm",),
        ("isolatedMM",),
        ("maintenance_margin",),
        ("maintenanceMargin",),
        ("info", "isolated_mm"),
        ("info", "isolatedMM"),
        ("info", "maintenance_margin"),
        ("info", "maintenanceMargin"),
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
    raw_isolated_im = extract_from_paths(entry, *isolated_im_candidates)
    isolated_im = decimal_from(raw_isolated_im)
    raw_isolated_mm = extract_from_paths(entry, *isolated_mm_candidates)
    isolated_mm = decimal_from(raw_isolated_mm)

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
        "isolated_im": decimal_to_str(isolated_im),
        "isolated_mm": decimal_to_str(isolated_mm),
    }
    return pnl_value, payload, signed_size


def load_single_account(*, label: str) -> AccountCredentials:
    label_clean = label.strip() or "default"

    def _env(key: str) -> Optional[str]:
        return os.getenv(f"GRVT_{key}")

    trading_account_id = _env("TRADING_ACCOUNT_ID")
    private_key = _env("PRIVATE_KEY")
    api_key = _env("API_KEY")
    if trading_account_id is None or private_key is None or api_key is None:
        raise ValueError(
            "Missing GRVT_TRADING_ACCOUNT_ID / GRVT_PRIVATE_KEY / GRVT_API_KEY. "
            "Set them in your environment or .env file."
        )

    env_name = (_env("ENVIRONMENT") or _env("ENV") or "prod").strip().lower()
    main_account_id = (_env("MAIN_ACCOUNT_ID") or os.getenv("GRVT_MAIN_ACCOUNT_ID") or "").strip() or None
    main_sub_account_raw = _env("MAIN_SUB_ACCOUNT_ID") or os.getenv("GRVT_MAIN_SUB_ACCOUNT_ID") or "0"
    trading_account_id_clean = trading_account_id.strip()
    main_sub_account_clean = str(main_sub_account_raw or "").strip()
    if not main_sub_account_clean or main_sub_account_clean == "0":
        main_sub_account_clean = trading_account_id_clean

    return AccountCredentials(
        label=label_clean,
        trading_account_id=trading_account_id_clean,
        private_key=private_key.strip(),
        api_key=api_key.strip(),
        environment=env_name,
        main_account_id=main_account_id,
        main_sub_account_id=main_sub_account_clean,
    )


def build_session(creds: AccountCredentials) -> AccountSession:
    GrvtCcxtCls, GrvtEnvCls = import_grvt_sdk()
    env_map = {
        "prod": GrvtEnvCls.PROD,
        "production": GrvtEnvCls.PROD,
        "testnet": GrvtEnvCls.TESTNET,
        "staging": GrvtEnvCls.STAGING,
        "dev": GrvtEnvCls.DEV,
    }
    resolved_env = env_map.get(creds.environment.lower(), GrvtEnvCls.PROD)
    client = GrvtCcxtCls(
        env=resolved_env,
        parameters={
            "trading_account_id": creds.trading_account_id,
            "private_key": creds.private_key,
            "api_key": creds.api_key,
        },
    )
    return AccountSession(
        label=creds.label,
        client=client,
        main_account_id=creds.main_account_id,
        sub_account_id=creds.trading_account_id,
        main_sub_account_id=creds.main_sub_account_id,
    )


class GrvtAccountMonitor:
    def __init__(
        self,
        *,
        session: AccountSession,
        coordinator_url: str,
        agent_id: str,
        poll_interval: float,
        request_timeout: float,
        max_positions: int,
        coordinator_username: Optional[str] = None,
        coordinator_password: Optional[str] = None,
        default_symbol: Optional[str] = None,
        default_transfer_currency: Optional[str] = None,
        default_transfer_direction: Optional[str] = None,
        default_transfer_type: Optional[str] = None,
        transfer_log_path: Optional[str] = None,
        disable_transfer_log: bool = False,
        transfer_api_variant: Optional[str] = None,
        control_ws: bool = True,
        control_ws_ping_interval: float = 20.0,
        control_ws_reconnect_delay: float = 3.0,
    ) -> None:
        self._session = session
        self._coordinator_url = coordinator_url.rstrip("/")
        self._agent_id = agent_id
        self._poll_interval = max(poll_interval, 2.0)
        self._timeout = max(request_timeout, 1.0)
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
        self._default_symbol = (
            (default_symbol or "").strip()
            or os.getenv("GRVT_DEFAULT_SYMBOL", "").strip()
            or os.getenv("GRVT_INSTRUMENT", "").strip()
        ) or None
        self._symbol_aliases: Dict[str, str] = {}
        self._processed_adjustments: Dict[str, Dict[str, Any]] = {}
        self._latest_positions: Dict[str, Decimal] = {}
        self._volatility_history: Dict[str, Deque[Tuple[float, Decimal]]] = {}
        self._volatility_last_sample: Dict[str, float] = {}
        self._currency_catalog: Dict[str, int] = {}
        self._control_endpoint = f"{self._coordinator_url}/control"
        self._update_endpoint = f"{self._coordinator_url}/update"
        self._ack_endpoint = f"{self._coordinator_url}/grvt/adjust/ack"
        self._ws_enabled = bool(control_ws)
        self._ws_connected = False
        self._ws_last_message = 0.0
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_stop = threading.Event()
        self._control_ws_ping_interval = float(control_ws_ping_interval)
        self._control_ws_reconnect_delay = float(control_ws_reconnect_delay)
        self._register_symbol_hint(self._default_symbol)
        self._home_main_account_id = session.main_account_id or os.getenv("GRVT_MAIN_ACCOUNT_ID")
        self._home_sub_account_id = session.sub_account_id or os.getenv("GRVT_TRADING_ACCOUNT_ID")
        main_sub_candidate = (
            session.main_sub_account_id
            or os.getenv("GRVT_MAIN_SUB_ACCOUNT_ID")
            or "0"
        )
        self._home_main_sub_account_id = str(main_sub_candidate or "").strip() or "0"
        if (
            (not self._home_main_sub_account_id or self._home_main_sub_account_id == "0")
            and self._home_sub_account_id
        ):
            self._home_main_sub_account_id = str(self._home_sub_account_id).strip()
        transfer_currency_env = os.getenv("GRVT_DEFAULT_TRANSFER_CURRENCY") or os.getenv("GRVT_TRANSFER_CURRENCY")
        transfer_direction_env = os.getenv("GRVT_DEFAULT_TRANSFER_DIRECTION") or os.getenv("GRVT_TRANSFER_DIRECTION")
        transfer_type_env = os.getenv("GRVT_DEFAULT_TRANSFER_TYPE") or os.getenv("GRVT_TRANSFER_TYPE")
        currency_source = default_transfer_currency or transfer_currency_env or "USDT"
        direction_source = default_transfer_direction or transfer_direction_env or "main_to_main"
        type_source = default_transfer_type or transfer_type_env or "INTERNAL"
        self._default_transfer_currency = str(currency_source).strip().upper()
        self._default_transfer_direction = str(direction_source).strip().lower()
        self._default_transfer_type = self._canonicalize_transfer_type_key(type_source)
        self._prime_currency_catalog()
        lite_metadata_flag = os.getenv("GRVT_LITE_INCLUDE_METADATA")
        if lite_metadata_flag is None:
            self._lite_include_transfer_metadata = True
        else:
            self._lite_include_transfer_metadata = str(lite_metadata_flag).strip().lower() in {"1", "true", "yes", "on"}
        self._transfer_log_path: Optional[Path] = self._resolve_transfer_log_path(
            transfer_log_path,
            disable_transfer_log,
        )
        self._transfer_api_variant = self._resolve_transfer_api_variant(
            transfer_api_variant
            or os.getenv("GRVT_TRANSFER_API")
            or os.getenv("GRVT_TRANSFER_API_VARIANT")
        )
        env_transfer_key = os.getenv("GRVT_TRANSFER_PRIVATE_KEY")
        self._transfer_private_key = env_transfer_key.strip() if env_transfer_key else None
        self._transfer_metadata_provider = (
            os.getenv("GRVT_TRANSFER_METADATA_PROVIDER")
            or os.getenv("GRVT_TRANSFER_PROVIDER")
            or (session.label if getattr(session, "label", None) else None)
        )
        self._transfer_metadata_chain_id = (
            os.getenv("GRVT_TRANSFER_METADATA_CHAIN_ID")
            or os.getenv("GRVT_TRANSFER_CHAIN_ID")
            or os.getenv("GRVT_CHAIN_ID")
        )
        self._transfer_metadata_endpoint = os.getenv("GRVT_TRANSFER_METADATA_ENDPOINT")
        enforce_flag = os.getenv("GRVT_ENFORCE_TRANSFER_METADATA_SCHEMA")
        if enforce_flag is None:
            self._enforce_transfer_metadata_schema = True
        else:
            self._enforce_transfer_metadata_schema = str(enforce_flag).strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        self._raw_transfer_client: Optional[Any] = None
        self._raw_transfer_client_config: Optional[Any] = None

    def _build_transfer_route(self, direction: str) -> Optional[Dict[str, str]]:
        direction_normalized = (direction or "").strip().lower()
        main_account = (self._home_main_account_id or "").strip()
        trading_sub = (self._home_sub_account_id or "").strip()
        main_sub = (self._home_main_sub_account_id or "").strip() or "0"

        if not main_account:
            return None

        if direction_normalized == "sub_to_main":
            if not trading_sub:
                return None
            return {
                "from_account_id": main_account,
                "from_sub_account_id": trading_sub,
                "to_account_id": main_account,
                "to_sub_account_id": main_sub,
            }
        if direction_normalized == "main_to_sub":
            if not trading_sub:
                return None
            return {
                "from_account_id": main_account,
                "from_sub_account_id": main_sub,
                "to_account_id": main_account,
                "to_sub_account_id": trading_sub,
            }
        if direction_normalized == "main_to_main":
            return {
                "from_account_id": main_account,
                "from_sub_account_id": main_sub,
                "to_account_id": main_account,
                "to_sub_account_id": main_sub,
            }
        return None

    def _build_transfer_defaults(self) -> Optional[Dict[str, Any]]:
        routes: Dict[str, Dict[str, str]] = {}
        for direction in ("sub_to_main", "main_to_sub", "main_to_main"):
            route = self._build_transfer_route(direction)
            if route:
                routes[direction] = route

        baseline_account = (self._home_main_account_id or "").strip()
        trading_sub = (self._home_sub_account_id or "").strip()
        defaults: Dict[str, Any] = {
            "agent_label": self._session.label,
            "main_account_id": baseline_account or None,
            "sub_account_id": trading_sub or None,
            "main_sub_account_id": (self._home_main_sub_account_id or "").strip() or None,
            "currency": self._default_transfer_currency,
            "direction": self._default_transfer_direction,
            "transfer_type": self._default_transfer_type,
        }
        defaults = {key: value for key, value in defaults.items() if value not in {None, ""}}
        if not defaults and not routes:
            return None
        if routes:
            defaults["routes"] = routes
        return defaults

    def _collect(self) -> Optional[Dict[str, Any]]:
        timestamp = time.time()
        try:
            positions = self._session.client.fetch_positions() or []
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.exception("Failed to fetch positions for %s: %s", self._session.label, exc)
            return None

        account_total = Decimal("0")
        account_eth = Decimal("0")
        account_btc = Decimal("0")
        position_rows: List[Dict[str, Any]] = []

        for raw_position in positions:
            pnl_value, position_payload, signed_size = compute_position_pnl(raw_position)
            account_total += pnl_value
            base = base_asset(position_payload.get("symbol", ""))
            if base == "ETH":
                account_eth += pnl_value
            elif base == "BTC":
                account_btc += pnl_value
            position_rows.append(position_payload)
            self._record_position(position_payload.get("symbol"), signed_size)
            mark_value = decimal_from(position_payload.get("mark_price"))
            self._record_volatility_sample(position_payload.get("symbol"), mark_value, timestamp)

        if self._max_positions is not None:
            position_rows = position_rows[: self._max_positions]

        balance_total: Optional[Decimal] = None
        balance_available: Optional[Decimal] = None
        account_summary: Dict[str, Any] = {}
        try:
            balance_payload = self._session.client.fetch_balance() or {}
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Failed to fetch balance for %s: %s", self._session.label, exc)
            balance_payload = {}

        try:
            account_summary = self._session.client.get_account_summary() or {}
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Failed to fetch account summary for %s: %s", self._session.label, exc)
            account_summary = {}
        if isinstance(account_summary, dict):
            if account_summary.get("maintenance_margin") is None or account_summary.get("initial_margin") is None:
                LOGGER.warning(
                    "GRVT account summary missing margin fields for %s (initial_margin=%s maintenance_margin=%s)",
                    self._session.label,
                    account_summary.get("initial_margin"),
                    account_summary.get("maintenance_margin"),
                )

        if isinstance(balance_payload, dict):
            total_raw = extract_from_paths(balance_payload, *BALANCE_TOTAL_PATHS)
            balance_total = decimal_from(total_raw)
            if balance_total is None:
                totals_block = balance_payload.get("total")
                if isinstance(totals_block, dict):
                    for key in ("USDT", "USD", "usdt", "usd"):
                        balance_total = decimal_from(totals_block.get(key))
                        if balance_total is not None:
                            break
            available_raw = extract_from_paths(balance_payload, *BALANCE_AVAILABLE_PATHS)
            balance_available = decimal_from(available_raw)
            if balance_available is None:
                free_block = balance_payload.get("free")
                if isinstance(free_block, dict):
                    for key in ("USDT", "USD", "usdt", "usd"):
                        balance_available = decimal_from(free_block.get(key))
                        if balance_available is not None:
                            break

        if balance_total is None and balance_available is not None:
            balance_total = balance_available
        if balance_available is None and balance_total is not None:
            balance_available = balance_total

        equity_total: Optional[Decimal] = None
        equity_available: Optional[Decimal] = None
        if balance_total is not None:
            equity_total = balance_total + account_total
        if balance_available is not None:
            equity_available = balance_available + account_total

        volatility_rankings = self._compute_volatility_rankings(timestamp)

        summary = {
            "account_count": 1,
            "total_pnl": decimal_to_str(account_total),
            "eth_pnl": decimal_to_str(account_eth),
            "btc_pnl": decimal_to_str(account_btc),
            "balance": decimal_to_str(balance_total),
            "available_balance": decimal_to_str(balance_available),
            "equity": decimal_to_str(equity_total),
            "available_equity": decimal_to_str(equity_available),
            "initial_margin": decimal_to_str(decimal_from(account_summary.get("initial_margin"))),
            "maintenance_margin": decimal_to_str(decimal_from(account_summary.get("maintenance_margin"))),
            "updated_at": timestamp,
        }

        payload = {
            "agent_id": self._agent_id,
            "instrument": f"GRVT {self._session.label}",
            "grvt_accounts": {
                "updated_at": timestamp,
                "summary": summary,
                "volatility_rankings": volatility_rankings,
                "volatility_meta": {
                    "sample_interval": VOLATILITY_SAMPLE_INTERVAL,
                    "windows": VOLATILITY_WINDOWS,
                    "topn": VOLATILITY_TOPN,
                },
                "accounts": [
                    {
                        "name": self._session.label,
                        "total_pnl": decimal_to_str(account_total),
                        "eth_pnl": decimal_to_str(account_eth),
                        "btc_pnl": decimal_to_str(account_btc),
                        "balance": decimal_to_str(balance_total),
                        "available_balance": decimal_to_str(balance_available),
                        "equity": decimal_to_str(equity_total),
                        "available_equity": decimal_to_str(equity_available),
                        "initial_margin": decimal_to_str(decimal_from(account_summary.get("initial_margin"))),
                        "maintenance_margin": decimal_to_str(decimal_from(account_summary.get("maintenance_margin"))),
                        "positions": position_rows,
                        "updated_at": timestamp,
                    }
                ],
            },
        }
        transfer_defaults = self._build_transfer_defaults()
        if transfer_defaults:
            payload["grvt_accounts"]["transfer_defaults"] = transfer_defaults
        return payload

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

    def _record_position(self, symbol: Optional[str], signed_size: Optional[Decimal]) -> None:
        if symbol is None or signed_size is None:
            return
        normalized = self._normalize_symbol_label(symbol)
        if not normalized:
            return
        self._register_symbol_hint(symbol)
        self._latest_positions[normalized] = signed_size

    def _record_volatility_sample(
        self,
        symbol: Optional[str],
        price: Optional[Decimal],
        timestamp: float,
    ) -> None:
        if symbol is None or price is None or price <= 0:
            return
        normalized = self._normalize_symbol_label(symbol)
        if not normalized:
            return
        last_ts = self._volatility_last_sample.get(normalized)
        if last_ts is not None and timestamp - last_ts < VOLATILITY_SAMPLE_INTERVAL:
            return
        self._volatility_last_sample[normalized] = timestamp
        history = self._volatility_history.setdefault(normalized, deque())
        history.append((timestamp, price))
        max_window = max(VOLATILITY_WINDOWS.values()) if VOLATILITY_WINDOWS else 0
        if max_window > 0:
            cutoff = timestamp - (max_window + VOLATILITY_SAMPLE_INTERVAL)
            while history and history[0][0] < cutoff:
                history.popleft()

    def _compute_volatility_rankings(self, now_ts: float) -> Dict[str, List[Dict[str, Any]]]:
        rankings: Dict[str, List[Dict[str, Any]]] = {key: [] for key in VOLATILITY_WINDOWS}
        for symbol, history in self._volatility_history.items():
            if not history:
                continue
            latest_ts, latest_price = history[-1]
            if latest_price <= 0:
                continue
            display_symbol = self._symbol_aliases.get(symbol, symbol)
            for key, window_sec in VOLATILITY_WINDOWS.items():
                target = now_ts - window_sec
                anchor_price: Optional[Decimal] = None
                for ts, price in history:
                    if ts <= target:
                        anchor_price = price
                    else:
                        break
                if anchor_price is None or anchor_price <= 0:
                    oldest_ts, oldest_price = history[0]
                    if oldest_price > 0 and oldest_ts < latest_ts:
                        anchor_price = oldest_price
                    else:
                        continue
                try:
                    change = (latest_price - anchor_price) / anchor_price
                except Exception:
                    continue
                change_pct = float(change * Decimal("100"))
                rankings[key].append({
                    "symbol": display_symbol,
                    "change_pct": change_pct,
                    "last_price": float(latest_price),
                    "abs_change": abs(change_pct),
                })

        for key, rows in rankings.items():
            rows.sort(key=lambda row: row.get("abs_change", 0), reverse=True)
            rankings[key] = rows[:VOLATILITY_TOPN]
        return rankings

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
        try:
            raw_positions = self._session.client.fetch_positions() or []
        except Exception as exc:
            LOGGER.warning("Failed to refresh GRVT positions for %s: %s", self._session.label, exc)
            return
        self._latest_positions = {}
        for entry in raw_positions:
            _, payload, signed_size = compute_position_pnl(entry)
            self._record_position(payload.get("symbol"), signed_size)

    def _register_symbol_hint(self, symbol: Optional[str]) -> None:
        if not symbol:
            return
        normalized = self._normalize_symbol_label(symbol)
        if not normalized:
            return
        if normalized not in self._symbol_aliases:
            self._symbol_aliases[normalized] = symbol

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
        if saw_all and self._default_symbol:
            return self._default_symbol
        if normalized_targets and not resolved:
            # Fallback to default if provided but no direct match
            if self._default_symbol:
                return self._default_symbol
        if not resolved and self._symbol_aliases:
            return next(iter(self._symbol_aliases.values()))
        return self._default_symbol

    def _fetch_agent_control(self) -> Optional[Dict[str, Any]]:
        try:
            response = self._http.get(
                self._control_endpoint,
                params={"agent_id": self._agent_id},
                timeout=self._timeout,
                auth=self._auth,
            )
        except RequestException as exc:
            raise RuntimeError(f"Failed to query coordinator control endpoint: {exc}") from exc
        if response.status_code >= 400:
            raise RuntimeError(
                f"Coordinator control query failed: HTTP {response.status_code} {response.text}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            snippet = response.text[:200]
            raise RuntimeError(f"Coordinator control response not JSON: {snippet}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"Unexpected control payload type: {type(payload).__name__}"
            )
        return payload

    def _place_market_order(self, symbol: str, side: str, quantity: Decimal) -> Any:
        client = self._session.client
        side = side.lower()
        if side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported order side '{side}'")
        amount_value = float(quantity)
        errors: List[str] = []
        try:
            return client.create_order(symbol, "market", side, amount_value)
        except AttributeError:
            errors.append("create_order unavailable")
        except Exception as exc:
            errors.append(f"create_order failed: {exc}")
        try:
            return client.create_market_order(symbol, side, amount_value)
        except AttributeError:
            errors.append("create_market_order unavailable")
        except Exception as exc:
            errors.append(f"create_market_order failed: {exc}")
        method_name = f"create_market_{side}_order"
        method = getattr(client, method_name, None)
        if method is not None:
            try:
                return method(symbol, amount_value)
            except Exception as exc:
                errors.append(f"{method_name} failed: {exc}")
        raise RuntimeError("; ".join(errors) or "No supported order placement method available")

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
            for key in ("id", "order_id", "client_order_id", "clientOrderId"):
                value = order.get(key)
                if value:
                    order_id = str(value)
                    break
        delta = trade_quantity if side == "buy" else -trade_quantity
        new_net = net_size + delta
        self._update_cached_position(symbol, new_net)
        note_core = (
            f"{side.upper()} {decimal_to_str(trade_quantity) or trade_quantity} {symbol} "
            f"(net {decimal_to_str(net_size) or net_size} -> {decimal_to_str(new_net) or new_net})"
        )
        if order_id:
            note_core = f"{note_core} (order {order_id})"
        if note_suffix:
            note_core = f"{note_core}; {note_suffix}"
        LOGGER.info("Executed GRVT adjustment via monitor: %s", note_core)
        return "acknowledged", note_core

    def _execute_transfer(self, entry: Dict[str, Any]) -> Tuple[str, str]:
        payload = self._prepare_transfer_payload(entry)
        response = self._call_transfer_endpoint(payload)
        transfer_id: Optional[str] = None
        if isinstance(response, dict):
            for key in ("transfer_id", "tx_id", "id", "request_id"):
                value = response.get(key)
                if value:
                    transfer_id = str(value)
                    break
        descriptor_from = self._format_transfer_descriptor(
            payload.get("from_account_id"), payload.get("from_sub_account_id")
        )
        descriptor_to = self._format_transfer_descriptor(
            payload.get("to_account_id"), payload.get("to_sub_account_id")
        )
        note = (
            f"TRANSFER {payload['num_tokens']} {payload['currency']} "
            f"{descriptor_from} -> {descriptor_to}"
        )
        if transfer_id:
            note = f"{note} (tx {transfer_id})"
        LOGGER.info("Executed GRVT transfer via monitor: %s", note)
        return "acknowledged", note

    def _prepare_transfer_payload(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        sources: List[Dict[str, Any]] = []
        for key in ("payload", "transfer"):
            block = entry.get(key)
            if isinstance(block, dict):
                sources.append(block)
        sources.append(entry)
        fields = (
            "from_account_id",
            "from_sub_account_id",
            "to_account_id",
            "to_sub_account_id",
            "currency",
            "num_tokens",
            "transfer_type",
        )
        for source in sources:
            for field in fields:
                value = source.get(field)
                if value is not None and field not in payload:
                    payload[field] = value
        metadata: Dict[str, Any] = {}
        for source in sources:
            meta = source.get("transfer_metadata") or source.get("metadata")
            if isinstance(meta, dict):
                metadata.update({k: v for k, v in meta.items() if v is not None})
        if metadata:
            payload["transfer_metadata"] = metadata
        magnitude = payload.get("num_tokens")
        if magnitude is None:
            magnitude = entry.get("magnitude")
        amount = decimal_from(magnitude)
        if amount is None or amount <= 0:
            raise ValueError(f"Invalid transfer amount '{magnitude}'")
        payload["num_tokens"] = decimal_to_str(amount)
        currency = payload.get("currency")
        if currency:
            payload["currency"] = str(currency).strip().upper()
        elif self._default_transfer_currency:
            payload["currency"] = self._default_transfer_currency
        else:
            raise ValueError("Transfer currency not specified and no default configured")
        if "transfer_type" not in payload and self._default_transfer_type:
            payload["transfer_type"] = self._default_transfer_type
        self._apply_default_transfer_targets(payload)
        required = (
            "from_account_id",
            "from_sub_account_id",
            "to_account_id",
            "to_sub_account_id",
        )
        for field in required:
            if not payload.get(field):
                raise ValueError(f"Transfer request missing required field '{field}'")
        payload["transfer_metadata"] = self._coerce_transfer_metadata_schema(
            payload.get("transfer_metadata"),
            entry,
            payload,
        )
        return payload

    def _apply_default_transfer_targets(self, payload: Dict[str, Any]) -> None:
        metadata = payload.get("transfer_metadata") or {}
        direction = str(
            metadata.get("direction")
            or payload.get("direction")
            or self._default_transfer_direction
        ).strip().lower()
        payload["direction"] = direction
        if not payload.get("from_account_id") and self._home_main_account_id:
            payload["from_account_id"] = self._home_main_account_id
        if not payload.get("from_sub_account_id"):
            default_from_sub = self._home_sub_account_id if direction != "main_to_sub" else self._home_main_sub_account_id
            payload["from_sub_account_id"] = default_from_sub or self._home_main_sub_account_id
        if not payload.get("to_account_id"):
            payload["to_account_id"] = self._home_main_account_id or payload.get("from_account_id")
        if not payload.get("to_sub_account_id"):
            if direction in {"sub_to_main", "default", "to_main"}:
                payload["to_sub_account_id"] = self._home_main_sub_account_id
            elif direction == "main_to_sub":
                payload["to_sub_account_id"] = self._home_sub_account_id or self._home_main_sub_account_id
            else:
                payload["to_sub_account_id"] = self._home_main_sub_account_id

    @staticmethod
    def _select_metadata_value(metadata: Mapping[str, Any], keys: Sequence[str]) -> Optional[Any]:
        for key in keys:
            if key in metadata:
                value = metadata[key]
                if not GrvtAccountMonitor._is_empty_transfer_value(value):
                    return value
        return None

    def _metadata_matches_expected_schema(self, metadata: Mapping[str, Any]) -> bool:
        required_keys: Tuple[Tuple[str, ...], ...] = (
            ("provider",),
            ("direction",),
            ("provider_tx_id", "providerTxId"),
            ("chainid", "chainId"),
            ("endpoint", "endpointAccountId"),
        )
        for key_group in required_keys:
            if not any(not self._is_empty_transfer_value(metadata.get(key)) for key in key_group):
                return False
        return True

    def _normalize_transfer_metadata_direction(self, direction: Any) -> str:
        text = str(direction or "").strip().lower()
        if text in {"sub_to_main", "withdraw", "withdrawal", "to_main"}:
            return "WITHDRAWAL"
        if text in {"main_to_sub", "deposit", "to_sub"}:
            return "DEPOSIT"
        if text in {"main_to_main", "internal", "transfer"}:
            return "INTERNAL"
        return "INTERNAL"

    def _coerce_transfer_metadata_schema(
        self,
        metadata: Optional[Mapping[str, Any]],
        entry: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> Dict[str, Any]:
        metadata_dict = dict(metadata or {})
        if not self._enforce_transfer_metadata_schema:
            return metadata_dict
        if metadata_dict and self._metadata_matches_expected_schema(metadata_dict):
            return self._ensure_transfer_metadata_aliases(metadata_dict)
        return self._build_structured_transfer_metadata(metadata_dict, entry, payload)

    def _ensure_transfer_metadata_aliases(self, metadata: Mapping[str, Any]) -> Dict[str, Any]:
        metadata_dict = dict(metadata or {})
        alias_groups: Tuple[Tuple[str, ...], ...] = (
            ("provider_tx_id", "providerTxId"),
            ("chainid", "chainId"),
            ("endpoint", "endpointAccountId"),
        )
        for group in alias_groups:
            selected_value: Optional[Any] = None
            for key in group:
                current_value = metadata_dict.get(key)
                if not self._is_empty_transfer_value(current_value):
                    selected_value = current_value
                    break
            if self._is_empty_transfer_value(selected_value):
                continue
            text_value = str(selected_value)
            for key in group:
                if self._is_empty_transfer_value(metadata_dict.get(key)):
                    metadata_dict[key] = text_value
        return metadata_dict

    def _build_structured_transfer_metadata(
        self,
        metadata: Mapping[str, Any],
        entry: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> Dict[str, Any]:
        metadata_dict = dict(metadata or {})
        provider = self._select_metadata_value(
            metadata_dict,
            ("provider", "transfer_provider", "agent_label", "agent_id"),
        )
        if self._is_empty_transfer_value(provider):
            provider = self._transfer_metadata_provider or self._agent_id or "COORDINATOR"
        provider_text = str(provider) if provider is not None else "COORDINATOR"
        request_id = entry.get("request_id") or metadata_dict.get("request_id")
        provider_tx_id = self._select_metadata_value(
            metadata_dict,
            ("provider_tx_id", "providerTxId", "tx_id", "txid", "transfer_id"),
        )
        if self._is_empty_transfer_value(provider_tx_id):
            provider_tx_id = request_id
        if self._is_empty_transfer_value(provider_tx_id):
            provider_tx_id = f"{self._agent_id}-{int(time.time() * 1000)}"
        provider_tx_text = str(provider_tx_id)
        direction_hint = self._select_metadata_value(
            metadata_dict,
            ("direction", "transfer_direction", "flow"),
        )
        if self._is_empty_transfer_value(direction_hint):
            direction_hint = payload.get("direction") or self._default_transfer_direction
        direction_text = self._normalize_transfer_metadata_direction(direction_hint)
        chain_id_value = self._select_metadata_value(
            metadata_dict,
            ("chainid", "chain_id", "chainId"),
        )
        if self._is_empty_transfer_value(chain_id_value):
            chain_id_value = self._transfer_metadata_chain_id or "42161"
        chain_id_text = str(chain_id_value) if chain_id_value is not None else "42161"
        endpoint_value = self._select_metadata_value(
            metadata_dict,
            ("endpoint", "funding_account", "to_account_id", "target_account_id"),
        )
        if self._is_empty_transfer_value(endpoint_value):
            endpoint_value = (
                payload.get("to_account_id")
                or payload.get("from_account_id")
                or self._transfer_metadata_endpoint
                or self._home_main_account_id
            )
        endpoint_text = self._format_checksum_address(endpoint_value) or endpoint_value
        endpoint_text = str(endpoint_text) if endpoint_text is not None else None
        structured = {
            "provider": provider_text,
            "direction": direction_text,
            "provider_tx_id": provider_tx_text,
            "chainid": chain_id_text,
            "endpoint": endpoint_text,
        }
        aliases = {
            "provider_tx_id": "providerTxId",
            "chainid": "chainId",
            "endpoint": "endpointAccountId",
        }
        metadata_with_aliases: Dict[str, Any] = {}
        for key, value in structured.items():
            if self._is_empty_transfer_value(value):
                continue
            text_value = str(value) if value is not None else value
            metadata_with_aliases[key] = text_value
            alias = aliases.get(key)
            if alias and alias not in metadata_with_aliases:
                metadata_with_aliases[alias] = text_value
        return metadata_with_aliases

    def _prime_currency_catalog(self) -> None:
        if not self._currency_catalog and GrvtCurrency is not None:
            try:
                for member in GrvtCurrency:  # type: ignore[not-an-iterable]
                    try:
                        self._currency_catalog[str(member.name).upper()] = int(member.value)
                    except Exception:
                        continue
            except Exception:
                LOGGER.debug("Failed to prime GRVT currency catalog from enum", exc_info=True)
        if DEFAULT_TRANSFER_CURRENCY not in self._currency_catalog:
            self._currency_catalog[DEFAULT_TRANSFER_CURRENCY] = DEFAULT_TRANSFER_CURRENCY_ID

    def _resolve_currency_id(self, currency: Optional[str]) -> int:
        symbol = str(currency or self._default_transfer_currency or DEFAULT_TRANSFER_CURRENCY).strip().upper()
        if not symbol:
            symbol = DEFAULT_TRANSFER_CURRENCY
        currency_id = self._currency_catalog.get(symbol)
        if currency_id is not None:
            return currency_id
        self._refresh_currency_catalog()
        currency_id = self._currency_catalog.get(symbol)
        if currency_id is not None:
            return currency_id
        LOGGER.warning(
            "Unknown GRVT currency '%s'; defaulting to %s (id %s)",
            symbol,
            DEFAULT_TRANSFER_CURRENCY,
            DEFAULT_TRANSFER_CURRENCY_ID,
        )
        return self._currency_catalog.get(DEFAULT_TRANSFER_CURRENCY, DEFAULT_TRANSFER_CURRENCY_ID)

    def _refresh_currency_catalog(self) -> None:
        base_url = self._resolve_grvt_private_url()
        if not base_url:
            return
        url = f"{base_url}/full/v1/currency"
        client = self._session.client
        http_session = getattr(client, "session", None)
        http_call = getattr(http_session, "get", None)
        if not callable(http_call):
            http_call = requests.get
        try:
            response = http_call(url, timeout=self._timeout)
            raise_for_status = getattr(response, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            data_method = getattr(response, "json", None)
            data = data_method() if callable(data_method) else None
        except Exception as exc:
            LOGGER.debug("Failed to refresh GRVT currency catalog: %s", exc)
            return
        results = None
        if isinstance(data, dict):
            results = data.get("result") or data.get("results")
        if not isinstance(results, list):
            return
        for entry in results:
            if not isinstance(entry, dict):
                continue
            symbol = entry.get("symbol")
            identifier = entry.get("id")
            if symbol and isinstance(identifier, int):
                self._currency_catalog[str(symbol).strip().upper()] = identifier

    def _resolve_grvt_private_url(self) -> Optional[str]:
        client = self._session.client
        urls = getattr(client, "urls", None)
        if isinstance(urls, dict):
            api_block = urls.get("api")
            if isinstance(api_block, dict):
                for key in ("private", "trade", "default", "rest"):
                    candidate = api_block.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        return candidate.rstrip("/")
        env_obj = getattr(client, "env", None)
        env_value = getattr(env_obj, "value", env_obj)
        if get_grvt_endpoint_domains and GrvtEndpointType is not None and env_value is not None:
            try:
                domains = get_grvt_endpoint_domains(env_value)  # type: ignore[misc]
            except Exception:
                domains = None
            if isinstance(domains, dict):
                trade_key = getattr(GrvtEndpointType, "TRADE_DATA", None)
                trade_domain = domains.get(trade_key)
                if hasattr(trade_key, "value") and not trade_domain:
                    trade_domain = domains.get(getattr(trade_key, "value", None))
                if not trade_domain:
                    trade_domain = domains.get("TRADE_DATA") or domains.get("trade_data")
                if isinstance(trade_domain, str) and trade_domain.strip():
                    return trade_domain.rstrip("/")
        return None

    @staticmethod
    def _serialize_transfer_metadata(metadata: Any) -> str:
        if metadata is None:
            return "{}"
        if isinstance(metadata, str):
            stripped = metadata.strip()
            return stripped or "{}"
        try:
            return json.dumps(metadata, separators=(",", ":"), sort_keys=True)
        except Exception:
            return str(metadata)

    @staticmethod
    def _sanitize_headers(headers: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        sanitized: Dict[str, Any] = {}
        if not headers:
            return sanitized
        sensitive_keys = {
            "authorization",
            "api-key",
            "x-api-key",
            "cookie",
            "set-cookie",
            "x-grvt-account-id",
        }
        for key, value in headers.items():
            if key is None:
                continue
            key_lower = str(key).lower()
            if key_lower in sensitive_keys:
                sanitized[str(key)] = "***"
            else:
                sanitized[str(key)] = value
        return sanitized

    def _sanitize_transfer_payload(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            redacted: Dict[str, Any] = {}
            for key, value in payload.items():
                key_lower = str(key).lower()
                if key_lower in {"private_key", "api_key", "signature", "r", "s", "v"}:
                    redacted[key] = "***"
                elif isinstance(value, (dict, list)):
                    redacted[key] = self._sanitize_transfer_payload(value)
                else:
                    redacted[key] = value
            return redacted
        if isinstance(payload, list):
            return [self._sanitize_transfer_payload(item) for item in payload]
        return payload

    def _log_transfer_request(
        self,
        context: str,
        method: str,
        url: str,
        headers: Optional[Mapping[str, Any]] = None,
        body: Any = None,
        json_payload: Optional[Any] = None,
    ) -> None:
        try:
            raw_headers: Optional[Dict[str, Any]] = None
            if headers:
                raw_headers = {str(key): value for key, value in headers.items() if key is not None}
            sanitized_headers = self._sanitize_headers(headers)
            body_repr: Any = None
            raw_body: Any = None
            if json_payload is not None:
                raw_body = json_payload
                body_repr = self._sanitize_transfer_payload(json_payload)
            elif body is not None:
                if isinstance(body, (bytes, bytearray)):
                    text = body.decode("utf-8", errors="replace")
                else:
                    text = str(body)
                parsed: Any = None
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
                if parsed is not None:
                    raw_body = parsed
                    body_repr = self._sanitize_transfer_payload(parsed)
                else:
                    raw_body = text
                    body_repr = text[:2048]
            LOGGER.info(
                "GRVT transfer request (%s): %s %s headers=%s body=%s",
                context,
                method,
                url,
                sanitized_headers,
                body_repr,
            )
            self._append_transfer_log_entry(context, method, url, raw_headers, raw_body)
        except Exception:
            LOGGER.debug("Failed to log GRVT transfer request", exc_info=True)

    @staticmethod
    def _summarize_http_error(response: Any) -> str:
        if response is None:
            return "HTTP request failed (no response)"
        status = getattr(response, "status_code", None)
        prefix = f"HTTP {status}" if status is not None else "HTTP error"
        detail: Optional[str] = None
        payload: Any = None
        if hasattr(response, "json"):
            try:
                payload = response.json()
            except ValueError:
                payload = None
        if isinstance(payload, dict):
            for key in ("error", "message", "detail", "reason", "status", "code"):
                value = payload.get(key)
                if value is None:
                    continue
                if isinstance(value, (str, int, float)):
                    detail = str(value)
                    break
                if isinstance(value, dict):
                    try:
                        detail = json.dumps(value, ensure_ascii=False)
                    except Exception:
                        detail = str(value)
                    break
        if detail is None:
            text = getattr(response, "text", None)
            if isinstance(text, str) and text.strip():
                detail = text.strip()
        if detail is None:
            content = getattr(response, "content", None)
            if isinstance(content, (bytes, bytearray)):
                snippet = content.decode("utf-8", errors="replace").strip()
                if snippet:
                    detail = snippet
        if detail:
            detail = detail[:400]
            return f"{prefix}: {detail}"
        return prefix

    def _append_transfer_log_entry(
        self,
        context: str,
        method: str,
        url: str,
        headers: Optional[Mapping[str, Any]],
        body: Any,
    ) -> None:
        path = self._transfer_log_path
        if path is None:
            return
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "context": context,
            "method": method,
            "url": url,
            "headers": headers,
            "body": body,
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False, separators=(",", ":"), default=str)
                handle.write("\n")
        except Exception:
            LOGGER.debug("Failed to append GRVT transfer audit log entry", exc_info=True)

    def _resolve_transfer_log_path(
        self,
        explicit_path: Optional[str],
        disable_flag: bool,
    ) -> Optional[Path]:
        def _env_true(name: str) -> bool:
            raw = os.getenv(name)
            if raw is None:
                return False
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        if disable_flag or _env_true("GRVT_DISABLE_TRANSFER_LOG"):
            return None
        candidate = explicit_path or os.getenv("GRVT_TRANSFER_LOG_FILE")
        if candidate:
            return Path(candidate).expanduser()
        label_source = self._agent_id or self._session.label or os.getenv("GRVT_ACCOUNT_LABEL") or "grvt-monitor"
        safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "-", label_source) or "grvt-monitor"
        log_dir = Path(os.getenv("GRVT_LOG_DIR", "logs")).expanduser()
        return log_dir / f"{safe_label}.transfer.log"

    @staticmethod
    def _resolve_transfer_api_variant(candidate: Optional[str]) -> str:
        if candidate:
            text = str(candidate).strip().lower()
            if text in {"lite", "full"}:
                return text
            LOGGER.warning("Unknown transfer API variant '%s'; defaulting to lite", candidate)
        return "lite"

    def _get_raw_transfer_client(self, config: Any) -> Any:
        if GrvtRawSync is None:  # pragma: no cover - optional dependency guard
            raise RuntimeError("Raw GRVT transfer client unavailable; install grvt-pysdk")
        cached_config = self._raw_transfer_client_config
        if (
            self._raw_transfer_client is not None
            and cached_config is not None
            and cached_config.env == config.env
            and cached_config.trading_account_id == config.trading_account_id
            and cached_config.private_key == config.private_key
            and cached_config.api_key == config.api_key
        ):
            return self._raw_transfer_client
        client = GrvtRawSync(config)
        self._raw_transfer_client = client
        self._raw_transfer_client_config = config
        return client

    def _resolve_transfer_private_key(self, client: Any) -> str:
        candidate = self._transfer_private_key or os.getenv("GRVT_TRANSFER_PRIVATE_KEY")
        if candidate:
            text = candidate.strip()
            if text:
                return text
        dedicated_attr = getattr(client, "_transfer_private_key", None) or getattr(client, "transfer_private_key", None)
        if dedicated_attr:
            text = str(dedicated_attr).strip()
            if text:
                return text
        raise RuntimeError(
            "Missing GRVT_TRANSFER_PRIVATE_KEY; set it in the environment for transfer signing"
        )

    @staticmethod
    def _is_empty_transfer_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value == ""
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) == 0
        return False

    @staticmethod
    def _format_checksum_address(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            text = str(value).strip()
        except Exception:
            return None
        if not text:
            return None
        if Web3 is not None:  # pragma: no cover - optional dependency guard
            try:
                return Web3.to_checksum_address(text)
            except (ValueError, AttributeError):
                return text
        return text

    def _prepare_transfer_payload_for_variant(
        self,
        api_variant: str,
        transfer_dict: Dict[str, Any],
        metadata_obj: Any,
        metadata_text: str,
    ) -> Tuple[str, Dict[str, Any]]:
        variant = str(api_variant or "lite").strip().lower()
        if variant != "full":
            lite_payload = self._convert_transfer_to_lite_payload(transfer_dict, metadata_obj, metadata_text)
            return "lite", lite_payload
        return "full", transfer_dict

    def _convert_transfer_to_lite_payload(
        self,
        transfer_dict: Dict[str, Any],
        metadata_obj: Any,
        metadata_text: str,
    ) -> Dict[str, Any]:
        from_account_id = transfer_dict.get("from_account_id")
        lite_payload: Dict[str, Any] = {
            "fa": from_account_id,
            "fs": transfer_dict.get("from_sub_account_id"),
            "ta": transfer_dict.get("to_account_id"),
            "ts": transfer_dict.get("to_sub_account_id"),
            "c": transfer_dict.get("currency"),
            "nt": transfer_dict.get("num_tokens"),
        }
        signature_block = transfer_dict.get("signature") or {}
        signer_value = signature_block.get("signer") or from_account_id
        preferred_signer = signer_value
        formatted_signer = self._format_checksum_address(preferred_signer) or preferred_signer
        lite_signature = {
            "s": formatted_signer,
            "r": signature_block.get("r"),
            "s1": signature_block.get("s"),
            "v": signature_block.get("v"),
            "e": signature_block.get("expiration"),
            "n": signature_block.get("nonce"),
        }
        chain_id = signature_block.get("chain_id") or transfer_dict.get("chain_id")
        if not self._is_empty_transfer_value(chain_id):
            lite_signature["ci"] = chain_id
        lite_payload["s"] = {
            key: value
            for key, value in lite_signature.items()
            if not self._is_empty_transfer_value(value)
        }

        transfer_type_value = transfer_dict.get("transfer_type")
        if self._is_empty_transfer_value(transfer_type_value):
            transfer_type_value = self._default_transfer_type or "UNSPECIFIED"
        if not self._is_empty_transfer_value(transfer_type_value):
            lite_payload["tt"] = str(transfer_type_value)

        include_metadata = getattr(self, "_lite_include_transfer_metadata", True)
        metadata_string: Optional[str] = None
        if not self._is_empty_transfer_value(metadata_text):
            metadata_string = str(metadata_text).strip()
        elif metadata_obj is not None and not self._is_empty_transfer_value(metadata_obj):
            try:
                metadata_string = json.dumps(metadata_obj, separators=(",", ":"), sort_keys=True)
            except Exception:
                metadata_string = str(metadata_obj)
        if include_metadata and metadata_string and metadata_string not in {"", "{}"}:
            lite_payload["tm"] = metadata_string

        return {
            key: value
            for key, value in lite_payload.items()
            if not self._is_empty_transfer_value(value)
        }

    def _resolve_raw_env(self, env_candidate: Any) -> Any:
        if RawGrvtEnv is None:  # pragma: no cover - optional dependency guard
            raise RuntimeError("RawGrvtEnv helpers unavailable")
        env_value = getattr(env_candidate, "value", None) or getattr(env_candidate, "name", None)
        text = str(env_value or "prod").strip()
        if not text:
            text = "prod"
        lower_text = text.lower()
        try:
            return RawGrvtEnv(lower_text)  # type: ignore[call-arg]
        except Exception:
            try:
                return RawGrvtEnv[text.upper()]  # type: ignore[index]
            except Exception:
                LOGGER.warning("Unknown GRVT environment '%s'; defaulting to PROD", text)
                return RawGrvtEnv.PROD  # type: ignore[attr-defined]

    @staticmethod
    def _canonicalize_transfer_type_key(candidate: Any) -> str:
        text = str(candidate or "").strip()
        if not text:
            return "UNSPECIFIED"
        normalized = re.sub(r"[\s-]+", "_", text)
        normalized = re.sub(r"_+", "_", normalized)
        normalized = normalized.strip("_")
        if not normalized:
            return "UNSPECIFIED"
        canonical = normalized.upper()
        return TRANSFER_TYPE_ALIASES.get(canonical, canonical)

    def _normalize_transfer_type(self, transfer_type_value: Any) -> Any:
        if TransferType is None:  # pragma: no cover - optional dependency guard
            raise RuntimeError("TransferType helpers unavailable")
        if isinstance(transfer_type_value, TransferType):
            return transfer_type_value
        if transfer_type_value is None:
            transfer_type_value = self._default_transfer_type or "UNSPECIFIED"
        source_value = transfer_type_value
        canonical_key = self._canonicalize_transfer_type_key(transfer_type_value)
        try:
            return TransferType[canonical_key]  # type: ignore[index]
        except Exception:
            pass
        raw_text = str(transfer_type_value).strip()
        if raw_text:
            try:
                return TransferType(raw_text)  # type: ignore[call-arg]
            except Exception:
                pass
        try:
            return TransferType(canonical_key)  # type: ignore[call-arg]
        except Exception as exc:
            raise ValueError(f"Unsupported transfer type '{source_value}'") from exc

    def _call_transfer_endpoint(self, payload: Dict[str, Any]) -> Any:
        try:
            return self._post_transfer_request(payload)
        except Exception as exc:
            raise RuntimeError(f"Direct transfer POST failed: {exc}") from exc

    def _post_transfer_request(self, payload: Dict[str, Any]) -> Any:
        client = self._session.client
        sign_method = getattr(client, "sign", None)
        if self._transfer_api_variant == "full" and callable(sign_method):
            return self._post_transfer_via_client_sign(client, sign_method, payload)
        return self._post_transfer_with_raw_signing(client, payload, self._transfer_api_variant)

    def _post_transfer_via_client_sign(
        self,
        client: Any,
    sign_method: Callable[..., Any],
        payload: Dict[str, Any],
    ) -> Any:
        signed_request = sign_method("full/v1/transfer", "private", "POST", payload)
        if not isinstance(signed_request, dict):
            raise RuntimeError("sign() returned unexpected payload")
        url = signed_request.get("url")
        method = signed_request.get("method", "POST")
        body = signed_request.get("body")
        headers = signed_request.get("headers") or {}
        if not url:
            raise RuntimeError("sign() did not provide URL for transfer request")
        session = getattr(client, "session", None)
        http_call = getattr(session, "request", None) if session is not None else None
        if not callable(http_call):
            http_call = requests.request
        self._log_transfer_request(
            "client-sign",
            method,
            url,
            headers,
            body=body,
        )
        response = cast(Any, http_call)(method, url, data=body, headers=headers, timeout=self._timeout)
        if hasattr(response, "raise_for_status"):
            try:
                response.raise_for_status()
            except Exception as exc:
                detail = self._summarize_http_error(response)
                raise RuntimeError(detail) from exc
        if hasattr(response, "json"):
            try:
                return response.json()
            except ValueError:
                pass
        return getattr(response, "text", response)

    def _post_transfer_with_raw_signing(self, client: Any, payload: Dict[str, Any], api_variant: str) -> Any:
        missing_helpers = [
            GrvtTransfer,
            GrvtSignature,
            sign_transfer,
            GrvtApiConfig,
            RawGrvtEnv,
            EthAccount,
            GrvtRawSync,
            ApiTransferRequest,
        ]
        if any(helper is None for helper in missing_helpers):  # pragma: no cover - import guard
            raise RuntimeError(
                "Raw transfer signing helpers unavailable; please upgrade grvt-pysdk to v0.2.1 or newer"
            )
        private_key = self._resolve_transfer_private_key(client)
        api_key = getattr(client, "_api_key", None) or os.getenv("GRVT_API_KEY")
        trading_account_id = None
        get_account_id = getattr(client, "get_trading_account_id", None)
        if callable(get_account_id):
            trading_account_id = get_account_id()
        raw_env = self._resolve_raw_env(getattr(client, "env", None))
        currency_id = self._resolve_currency_id(payload.get("currency"))
        transfer_type_enum = self._normalize_transfer_type(payload.get("transfer_type"))
        signature_nonce = random.randint(1, 2**32 - 1)
        expiration_ns = int((time.time() + SIGNATURE_TTL_SECONDS) * 1_000_000_000)
        signature = GrvtSignature(  # type: ignore[call-arg]
            signer="",
            r="",
            s="",
            v=0,
            expiration=str(expiration_ns),
            nonce=signature_nonce,
        )
        metadata_obj = payload.get("transfer_metadata")
        metadata_text = self._serialize_transfer_metadata(metadata_obj)
        transfer = GrvtTransfer(  # type: ignore[call-arg]
            from_account_id=str(payload["from_account_id"]),
            from_sub_account_id=str(payload["from_sub_account_id"]),
            to_account_id=str(payload["to_account_id"]),
            to_sub_account_id=str(payload["to_sub_account_id"]),
            currency=str(payload["currency"]),
            num_tokens=str(payload["num_tokens"]),
            signature=signature,
            transfer_type=transfer_type_enum,
            transfer_metadata=metadata_text,
        )
        config = GrvtApiConfig(  # type: ignore[call-arg]
            env=raw_env,
            trading_account_id=trading_account_id,
            private_key=str(private_key),
            api_key=api_key,
            logger=LOGGER,
        )
        account = EthAccount.from_key(str(private_key))  # type: ignore[arg-type]
        signed_transfer = sign_transfer(  # type: ignore[misc]
            transfer,
            config,
            account,
            currencyId=currency_id,
        )
        transfer_dict = asdict(signed_transfer)
        transfer_dict["transfer_type"] = transfer_type_enum.value
        transfer_dict["transfer_metadata"] = metadata_text
        signature_block = transfer_dict.get("signature")
        if isinstance(signature_block, dict):
            signature_block["expiration"] = str(expiration_ns)
            signature_block["nonce"] = signature_nonce
        full_request_payload = dict(transfer_dict)
        try:
            full_request_payload["transfer_metadata"] = json.loads(metadata_text)
        except Exception:
            full_request_payload["transfer_metadata"] = metadata_text
        variant_key, request_payload = self._prepare_transfer_payload_for_variant(
            api_variant,
            transfer_dict,
            metadata_obj,
            metadata_text,
        )
        raw_client = self._get_raw_transfer_client(config)
        env_trade_endpoint = getattr(getattr(raw_client, "env", None), "trade_data", None)
        env_trade_url = getattr(env_trade_endpoint, "rpc_endpoint", None)
        base_url = env_trade_url or self._resolve_grvt_private_url()
        normalized_base_url = base_url.rstrip("/") if isinstance(base_url, str) and base_url else None

        def _execute_full_transfer() -> Dict[str, Any]:
            full_url = (
                f"{normalized_base_url}/full/v1/transfer"
                if normalized_base_url
                else "sdk://full/v1/transfer"
            )
            self._log_transfer_request(
                "raw-sign",
                "POST",
                full_url,
                json_payload=full_request_payload,
            )
            api_request = ApiTransferRequest(  # type: ignore[call-arg]
                from_account_id=transfer.from_account_id,
                from_sub_account_id=transfer.from_sub_account_id,
                to_account_id=transfer.to_account_id,
                to_sub_account_id=transfer.to_sub_account_id,
                currency=transfer.currency,
                num_tokens=transfer.num_tokens,
                signature=transfer.signature,
                transfer_type=transfer.transfer_type,
                transfer_metadata=transfer.transfer_metadata,
            )
            response_obj = raw_client.transfer_v1(api_request)
            if isinstance(response_obj, GrvtError):
                raise RuntimeError(
                    f"GRVT transfer rejected: code={response_obj.code} status={response_obj.status} message={response_obj.message}"
                )
            return asdict(response_obj)

        fallback_to_full = False
        response_dict: Dict[str, Any]
        if variant_key == "lite":
            if not normalized_base_url:
                raise RuntimeError("Unable to resolve GRVT trade endpoint for lite transfer requests")
            session = getattr(raw_client, "_session", None)
            if session is None:
                raise RuntimeError("Raw GRVT transfer client session unavailable for lite requests")
            try:
                raw_client._refresh_cookie()
            except Exception as exc:
                raise RuntimeError(f"Failed to refresh GRVT auth cookie: {exc}") from exc
            lite_url = f"{normalized_base_url}/lite/v1/transfer"
            session_headers = getattr(session, "headers", None)
            self._log_transfer_request(
                "raw-sign",
                "POST",
                lite_url,
                headers=session_headers,
                json_payload=request_payload,
            )
            response_obj = session.post(
                lite_url,
                json=request_payload,
                timeout=self._timeout,
            )
            if response_obj.status_code >= 400:
                detail = self._summarize_http_error(response_obj)
                if response_obj.status_code in {401, 403}:
                    LOGGER.warning("Lite transfer rejected (%s); retrying via full/v1/transfer", detail)
                    fallback_to_full = True
                else:
                    raise RuntimeError(f"GRVT lite transfer failed: {detail}")
            try:
                response_dict = cast(Dict[str, Any], response_obj.json())
            except ValueError:
                response_dict = {"raw": getattr(response_obj, "text", "")}
            if fallback_to_full:
                response_dict = _execute_full_transfer()
        else:
            response_dict = _execute_full_transfer()
        result_block = response_dict.get("result")
        if isinstance(result_block, dict):
            response_dict.update(result_block)
        return response_dict

    @staticmethod
    def _format_transfer_descriptor(account_id: Optional[str], sub_id: Optional[str]) -> str:
        account_text = (account_id or "?")
        sub_text = (sub_id or "?")
        return f"account={account_text} sub={sub_text}"

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

    def _build_control_ws_url(self) -> str:
        base = (self._coordinator_url or "").rstrip("/")
        if base.startswith("https://"):
            base = "wss://" + base[len("https://") :]
        elif base.startswith("http://"):
            base = "ws://" + base[len("http://") :]
        elif not base.startswith("ws://") and not base.startswith("wss://"):
            base = "ws://" + base
        return f"{base}/ws/grvt/control?agent_id={self._agent_id}"

    def _build_control_ws_headers(self) -> List[str]:
        username = (self._auth.username if self._auth else "") or ""
        password = (self._auth.password if self._auth else "") or ""
        if not username and not password:
            return []
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
        return [f"Authorization: Basic {token}"]

    def _handle_ws_message(self, message: str) -> None:
        self._ws_last_message = time.time()
        try:
            payload = json.loads(message)
        except Exception:
            LOGGER.debug("WS message not JSON; ignoring")
            return
        if not isinstance(payload, dict):
            return
        if "pending_adjustments" in payload:
            self._process_adjustments_snapshot(payload)

    def _control_ws_loop(self) -> None:
        if websocket is None:
            LOGGER.warning("websocket-client not available; fallback to polling")
            self._ws_enabled = False
            return

        delay = max(self._control_ws_reconnect_delay, 0.5)
        while not self._ws_stop.is_set():
            url = self._build_control_ws_url()

            def _on_open(_):
                self._ws_connected = True
                self._ws_last_message = time.time()
                LOGGER.info("GRVT control WS connected: %s", url)

            def _on_message(_, msg: str):
                self._handle_ws_message(msg)

            def _on_error(_, err: Exception):
                LOGGER.warning("GRVT control WS error: %s", err)

            def _on_close(_, status: int, msg: str):
                self._ws_connected = False
                LOGGER.info("GRVT control WS closed: %s %s", status, msg)

            app = websocket.WebSocketApp(
                url,
                header=self._build_control_ws_headers() or None,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )

            try:
                app.run_forever(
                    ping_interval=max(self._control_ws_ping_interval, 5.0),
                    ping_timeout=max(self._timeout, 5.0),
                )
            except Exception as exc:
                LOGGER.warning("GRVT control WS run failed: %s", exc)
                self._ws_connected = False

            if self._ws_stop.is_set():
                break
            time.sleep(min(delay, 30.0))

    def _start_control_ws_listener(self) -> None:
        if not self._ws_enabled:
            return
        if self._ws_thread and self._ws_thread.is_alive():
            return
        self._ws_stop.clear()
        self._ws_thread = threading.Thread(
            target=self._control_ws_loop,
            name=f"grvt-control-ws-{self._agent_id}",
            daemon=True,
        )
        self._ws_thread.start()

    def _should_poll_control(self) -> bool:
        if not self._ws_enabled:
            return True
        if not self._ws_connected:
            return True
        age = time.time() - (self._ws_last_message or 0)
        return age > max(self._poll_interval * 2, 10.0)

    def _process_adjustments_snapshot(self, snapshot: Dict[str, Any]) -> None:
        agent_block = snapshot.get("agent") if isinstance(snapshot, dict) else None
        pending = None
        if isinstance(agent_block, dict):
            pending = agent_block.get("pending_adjustments")
        if pending is None:
            pending = snapshot.get("pending_adjustments") if isinstance(snapshot, dict) else None
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

    def _process_adjustments(self) -> None:
        if not self._should_poll_control():
            return
        try:
            snapshot = self._fetch_agent_control()
        except Exception as exc:
            LOGGER.debug("Skipping adjustment processing; control fetch failed: %s", exc)
            return
        if not snapshot:
            return
        self._process_adjustments_snapshot(snapshot)

    def _prune_processed_adjustments(self, ttl: float = 3600.0) -> None:
        if not self._processed_adjustments:
            return
        cutoff = time.time() - max(ttl, 60.0)
        for request_id, record in list(self._processed_adjustments.items()):
            if record.get("acked") and record.get("timestamp", 0) < cutoff:
                self._processed_adjustments.pop(request_id, None)

    def _push(self, payload: Dict[str, Any]) -> None:
        try:
            response = self._http.post(
                self._update_endpoint,
                json=payload,
                timeout=self._timeout,
                auth=self._auth,
            )
        except RequestException as exc:
            raise RuntimeError(f"Failed to push monitor payload: {exc}") from exc
        if response.status_code >= 400:
            raise RuntimeError(f"Coordinator rejected payload: HTTP {response.status_code} {response.text}")

    def run_once(self) -> None:
        payload = self._collect()
        if payload is None:
            LOGGER.warning("Skipping coordinator update; unable to collect account data")
        else:
            self._push(payload)
            LOGGER.info(
                "Pushed GRVT monitor snapshot for %s (PnL %s)",
                self._session.label,
                payload["grvt_accounts"]["summary"].get("total_pnl"),
            )
        self._process_adjustments()

    def run_forever(self) -> None:
        LOGGER.info("Starting GRVT monitor for account %s", self._session.label)
        self._start_control_ws_listener()
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

    parser = argparse.ArgumentParser(description="Monitor a GRVT account per VPS and forward PnL data to the dashboard")
    parser.add_argument(
        "--coordinator-url",
        required=True,
        help="Hedge coordinator base URL, e.g. http://localhost:8899",
    )
    parser.add_argument("--coordinator-username", help="Optional Basic Auth username for coordinator access")
    parser.add_argument("--coordinator-password", help="Optional Basic Auth password for coordinator access")
    parser.add_argument("--agent-id", default="grvt-monitor", help="Agent identifier reported to the coordinator")
    parser.add_argument(
        "--account-label",
        default=os.getenv("GRVT_ACCOUNT_LABEL", "default"),
        help="Label shown on the dashboard for this VPS/account",
    )
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_SECONDS, help="Seconds between refreshes")
    parser.add_argument("--request-timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout for coordinator updates")
    parser.add_argument(
        "--control-ws",
        default=_env_bool("GRVT_CONTROL_WS", True),
        action="store_true",
        help="Enable coordinator control WebSocket for low-latency adjustments",
    )
    parser.add_argument(
        "--no-control-ws",
        dest="control_ws",
        action="store_false",
        help="Disable coordinator control WebSocket and fall back to polling",
    )
    parser.add_argument(
        "--control-ws-ping-interval",
        type=float,
        default=float(os.getenv("GRVT_CONTROL_WS_PING_INTERVAL", "20")),
        help="Seconds between WebSocket pings",
    )
    parser.add_argument(
        "--control-ws-reconnect-delay",
        type=float,
        default=float(os.getenv("GRVT_CONTROL_WS_RECONNECT_DELAY", "3")),
        help="Seconds to wait before reconnecting control WebSocket",
    )
    parser.add_argument(
        "--max-positions",
        type=int,
        default=MAX_ACCOUNT_POSITIONS,
        help="Maximum positions to include per account in the payload (0 = all)",
    )
    parser.add_argument(
        "--default-symbol",
        help="Fallback GRVT symbol/instrument to trade when adjustments omit explicit symbols",
    )
    parser.add_argument(
        "--main-account-id",
        help="Main account identifier for the GRVT sub account (fallback for transfer requests)",
    )
    parser.add_argument(
        "--main-sub-account-id",
        default=os.getenv("GRVT_MAIN_SUB_ACCOUNT_ID", "0"),
        help="Main account sub-account identifier (usually '0')",
    )
    parser.add_argument(
        "--default-transfer-currency",
        help="Default currency ticker for GRVT transfers when UI does not specify one",
    )
    parser.add_argument(
        "--default-transfer-direction",
        choices=["sub_to_main", "main_to_sub", "sub_to_sub", "default"],
        help="Default logical direction for GRVT transfers",
    )
    parser.add_argument(
        "--default-transfer-type",
        help="Default GRVT transfer type label (e.g. INTERNAL, WITHDRAWAL)",
    )
    parser.add_argument(
        "--transfer-api",
        choices=["full", "lite"],
        default=os.getenv("GRVT_TRANSFER_API")
        or os.getenv("GRVT_TRANSFER_API_VARIANT")
        or "lite",
        help="GRVT transfer API variant to target (full or lite). Defaults to lite or GRVT_TRANSFER_API env.",
    )
    parser.add_argument("--once", action="store_true", help="Collect and push a single snapshot, then exit")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument(
        "--log-file",
        help="Optional path to write agent logs (rotating file handler)",
    )
    parser.add_argument(
        "--transfer-log-file",
        help="Optional path to append sanitized GRVT transfer POST requests",
    )
    parser.add_argument(
        "--log-file-max-bytes",
        type=int,
        default=int(os.getenv("GRVT_LOG_FILE_MAX_BYTES", "5242880")),
        help="Rotate log file after this many bytes (default 5 MiB)",
    )
    parser.add_argument(
        "--log-file-backup-count",
        type=int,
        default=int(os.getenv("GRVT_LOG_FILE_BACKUP_COUNT", "5")),
        help="Number of rotated log files to keep",
    )
    parser.add_argument(
        "--no-log-file",
        action="store_true",
        default=_env_bool("GRVT_DISABLE_LOG_FILE", False),
        help="Disable file logging even if defaults would enable it",
    )
    parser.add_argument(
        "--no-transfer-log",
        action="store_true",
        default=_env_bool("GRVT_DISABLE_TRANSFER_LOG", False),
        help="Disable writing sanitized transfer request payloads to a log file",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Shortcut for --log-level WARNING to reduce log noise",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        help="Env file to preload (defaults to .env if present). Repeat to load multiple files.",
    )
    return parser.parse_args(argv)


def _resolve_log_file_path(args: argparse.Namespace) -> Optional[Path]:
    if getattr(args, "no_log_file", False):
        return None
    explicit = getattr(args, "log_file", None) or os.getenv("GRVT_LOG_FILE")
    if explicit:
        return Path(explicit).expanduser()
    agent_label = args.agent_id or os.getenv("GRVT_ACCOUNT_LABEL") or "grvt-monitor"
    safe_agent = re.sub(r"[^A-Za-z0-9_.-]+", "-", agent_label) or "grvt-monitor"
    log_dir = Path(os.getenv("GRVT_LOG_DIR", "logs")).expanduser()
    return log_dir / f"{safe_agent}.log"


def _configure_logging(args: argparse.Namespace) -> None:
    log_level_name = args.log_level
    if getattr(args, "quiet", False):
        log_level_name = "WARNING"
    level = getattr(logging, (log_level_name or "INFO").upper(), logging.INFO)
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))
    handlers: List[logging.Handler] = [stream_handler]

    log_file_path = _resolve_log_file_path(args)
    if log_file_path is not None:
        max_bytes = max(1024, int(args.log_file_max_bytes or 0))
        backup_count = max(1, int(args.log_file_backup_count or 1))
        try:
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                log_file_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
        except OSError as exc:
            print(f"Failed to set up log file {log_file_path}: {exc}", file=sys.stderr)

    logging.basicConfig(level=level, handlers=handlers, format=log_format)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    _configure_logging(args)

    env_files = args.env_file if args.env_file is not None else [".env"]
    load_env_files(env_files)

    try:
        credentials = load_single_account(label=args.account_label)
    except ValueError as exc:
        LOGGER.error(str(exc))
        sys.exit(1)
    if args.main_account_id or args.main_sub_account_id:
        credentials = replace(
            credentials,
            main_account_id=(args.main_account_id or credentials.main_account_id),
            main_sub_account_id=(args.main_sub_account_id or credentials.main_sub_account_id),
        )

    coordinator_username = args.coordinator_username or os.getenv("COORDINATOR_USERNAME")
    coordinator_password = args.coordinator_password or os.getenv("COORDINATOR_PASSWORD")
    default_symbol = (
        args.default_symbol
        or os.getenv("GRVT_DEFAULT_SYMBOL")
        or os.getenv("GRVT_INSTRUMENT")
    )

    session = build_session(credentials)
    monitor = GrvtAccountMonitor(
        session=session,
        coordinator_url=args.coordinator_url,
        agent_id=args.agent_id,
        poll_interval=args.poll_interval,
        request_timeout=args.request_timeout,
        max_positions=args.max_positions,
        coordinator_username=coordinator_username,
        coordinator_password=coordinator_password,
        default_symbol=default_symbol,
        default_transfer_currency=args.default_transfer_currency,
        default_transfer_direction=args.default_transfer_direction,
        default_transfer_type=args.default_transfer_type,
        transfer_log_path=args.transfer_log_file,
        disable_transfer_log=args.no_transfer_log,
        transfer_api_variant=args.transfer_api,
        control_ws=args.control_ws,
        control_ws_ping_interval=args.control_ws_ping_interval,
        control_ws_reconnect_delay=args.control_ws_reconnect_delay,
    )

    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
