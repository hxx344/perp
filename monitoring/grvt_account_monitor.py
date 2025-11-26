#!/usr/bin/env python3
"""GRVT account monitor.

Each VPS runs this helper for *its* GRVT account. The script polls positions,
summarises PnL, and forwards everything to the hedge coordinator so the
dashboard can show per-VPS health without juggling multiple account flags.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

getcontext().prec = 28

LOGGER = logging.getLogger("monitor.grvt_accounts")
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


@dataclass
class AccountSession:
    label: str
    client: Any


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
    text = symbol.replace(":", "/")
    parts = text.split("/")
    if not parts:
        return None
    return parts[0].upper().strip() or None


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

    return AccountCredentials(
        label=label_clean,
        trading_account_id=trading_account_id.strip(),
        private_key=private_key.strip(),
        api_key=api_key.strip(),
        environment=env_name,
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
    return AccountSession(label=creds.label, client=client)


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
    ) -> None:
        self._session = session
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
        self._default_symbol = (
            (default_symbol or "").strip()
            or os.getenv("GRVT_DEFAULT_SYMBOL", "").strip()
            or os.getenv("GRVT_INSTRUMENT", "").strip()
        ) or None
        self._symbol_aliases: Dict[str, str] = {}
        self._processed_adjustments: Dict[str, Dict[str, Any]] = {}
        self._latest_positions: Dict[str, Decimal] = {}
        self._control_endpoint = f"{self._coordinator_url}/control"
        self._update_endpoint = f"{self._coordinator_url}/update"
        self._ack_endpoint = f"{self._coordinator_url}/grvt/adjust/ack"
        self._register_symbol_hint(self._default_symbol)

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

        position_rows = position_rows[: self._max_positions]

        balance_total: Optional[Decimal] = None
        balance_available: Optional[Decimal] = None
        try:
            balance_payload = self._session.client.fetch_balance() or {}
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Failed to fetch balance for %s: %s", self._session.label, exc)
            balance_payload = {}

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
            "instrument": f"GRVT {self._session.label}",
            "grvt_accounts": {
                "updated_at": timestamp,
                "summary": summary,
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
                        "positions": position_rows,
                        "updated_at": timestamp,
                    }
                ],
            },
        }
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
        try:
            snapshot = self._fetch_agent_control()
        except Exception as exc:
            LOGGER.debug("Skipping adjustment processing; control fetch failed: %s", exc)
            return
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
    parser.add_argument("--max-positions", type=int, default=MAX_ACCOUNT_POSITIONS, help="Maximum positions to include per account in the payload")
    parser.add_argument(
        "--default-symbol",
        help="Fallback GRVT symbol/instrument to trade when adjustments omit explicit symbols",
    )
    parser.add_argument("--once", action="store_true", help="Collect and push a single snapshot, then exit")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument(
        "--env-file",
        action="append",
        help="Env file to preload (defaults to .env if present). Repeat to load multiple files.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    env_files = args.env_file if args.env_file is not None else [".env"]
    load_env_files(env_files)

    try:
        credentials = load_single_account(label=args.account_label)
    except ValueError as exc:
        LOGGER.error(str(exc))
        sys.exit(1)

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
    )

    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
