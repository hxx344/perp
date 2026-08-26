"""Robinhood Chain Lighter two-account neutral-position manager.

The manager is intentionally conservative:

* account reads use the public RH Lighter account endpoint;
* transfers and reduce-only closes are disabled unless ``--live`` is set;
* automatic transfers additionally require ``--auto-transfer``;
* the two configured accounts must advertise the same L1 master address before
  an intra-master transfer can be considered;
* every write is serialized and followed by a fresh reconciliation cycle.

The four-leg layout is configurable: the master can be long either SPY or
QQQ, while the subaccount always carries the opposite sign for both symbols.

Market ids and quantities are runtime configuration because RH market indexes
and contract precision are not stable values to hard-code in a strategy.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import logging
import math
import os
import random
import signal
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

import aiohttp
import dotenv

from exchanges.lighter_endpoints import ROBINHOOD_MAINNET, resolve_lighter_endpoint_profile

try:  # Keep pure risk helpers importable in environments without the SDK.
    from lighter.signer_client import SignerClient
except Exception:  # pragma: no cover - exercised on minimal read-only installs
    SignerClient = None  # type: ignore[assignment,misc]


LOGGER = logging.getLogger("strategies.rh_neutral_manager")
# RH calls the collateral USDG; the SDK still exposes the historical USDC
# constant/name for asset id 3.
USDG_ASSET_ID = 3
USDC_ASSET_ID = USDG_ASSET_ID
ROUTE_PERPS = 0
DEFAULT_TRANSFER_FEE_RAW = 0
MAX_ACTION_ID_CACHE = 512
# RH exposes both main accounts and subaccounts as tradable internal
# accounts.  Other account types are reserved pools/system accounts and must
# never be selected for monitoring or signed writes.
TRADABLE_ACCOUNT_TYPES = frozenset({0, 1})


class LighterWriteUncertainError(RuntimeError):
    """The signer call may have reached the sequencer but lacks a result."""

    def __init__(self, message: str, *, metadata: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message)
        self.metadata = dict(metadata or {})


class NeutralJournalError(RuntimeError):
    """The write journal could not be durably updated."""


class _NeutralInstanceLock:
    """Cross-platform advisory lock for one manager state file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: Any = None
        self._backend: Optional[str] = None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                backend = "msvcrt"
            elif os.name == "posix":
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                backend = "fcntl"
            else:  # pragma: no cover
                raise OSError(f"unsupported platform: {os.name}")
        except (BlockingIOError, OSError) as exc:
            handle.close()
            raise RuntimeError(f"another RH neutral manager owns lock {self.path.resolve()}") from exc
        self._handle = handle
        self._backend = backend
        try:
            handle.seek(0)
            handle.truncate()
            handle.write((json.dumps({"pid": os.getpid(), "started_at": int(time.time())}) + "\n").encode("ascii"))
            handle.flush()
            os.fsync(handle.fileno())
            if backend == "msvcrt":
                handle.seek(0)
        except OSError:
            pass

    def release(self) -> None:
        handle, backend = self._handle, self._backend
        self._handle = None
        self._backend = None
        if handle is None:
            return
        try:
            if backend == "msvcrt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            elif backend == "fcntl":
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


class _NeutralInstanceLockGroup:
    """Acquire all aliases that identify an account group.

    A configuration may identify the same accounts by L1 address or by
    explicit indexes.  Holding both aliases prevents one process from
    bypassing another merely by choosing a different discovery form.
    """

    def __init__(self, paths: Sequence[Path]) -> None:
        unique = {Path(path) for path in paths}
        self.paths = tuple(sorted(unique, key=lambda path: str(path)))
        self._locks = [_NeutralInstanceLock(path) for path in self.paths]

    def acquire(self) -> None:
        acquired: List[_NeutralInstanceLock] = []
        try:
            for lock in self._locks:
                lock.acquire()
                acquired.append(lock)
        except Exception:
            for lock in reversed(acquired):
                lock.release()
            raise

    def add_and_acquire(self, paths: Sequence[Path]) -> None:
        """Acquire newly discovered aliases while retaining existing locks."""

        existing = set(self.paths)
        new_paths = sorted({Path(path) for path in paths} - existing, key=lambda path: str(path))
        acquired: List[_NeutralInstanceLock] = []
        try:
            for path in new_paths:
                lock = _NeutralInstanceLock(path)
                lock.acquire()
                acquired.append(lock)
            self.paths = tuple(sorted((*self.paths, *new_paths), key=lambda path: str(path)))
            self._locks.extend(acquired)
        except Exception:
            for lock in reversed(acquired):
                lock.release()
            raise

    def release(self) -> None:
        for lock in reversed(self._locks):
            lock.release()


def _neutral_lock_path(identity: str) -> Path:
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
    return Path(tempfile.gettempdir()) / f"rh-neutral-{digest}.lock"


def _neutral_identity_lock_paths(settings: "NeutralSettings") -> Tuple[Path, ...]:
    """Return L1 and per-account aliases for one RH deployment.

    Per-account aliases intentionally use individual indexes instead of only
    the sorted pair.  Thus a process configured with one explicit account
    index cannot overlap a process configured with the same account plus a
    discovered peer.  L1 and index aliases are both held when available.
    """

    base = f"{settings.rest_url}|{settings.chain_id}|"
    paths: List[Path] = []
    if settings.l1_address:
        paths.append(_neutral_lock_path(base + "l1:" + settings.l1_address.casefold()))
    indexes = sorted({
        int(index)
        for index in (settings.main.account_index, settings.sub.account_index)
        if int(index) >= 0
    })
    for index in indexes:
        paths.append(_neutral_lock_path(base + f"account:{index}"))
    if not paths:
        # Validation normally prevents this, but retain a deterministic lock
        # for callers constructing settings before validation.
        paths.append(_neutral_lock_path(base + "unknown"))
    return tuple(sorted(set(paths), key=lambda path: str(path)))


def _neutral_identity_lock_path(settings: "NeutralSettings") -> Path:
    """Derive one host-wide lock name for a RH account group.

    The journal path is operator-configurable, so it cannot be the only lock:
    two processes using different state files must still not write the same
    accounts.  An L1 address is intentionally sufficient when discovery is in
    use; this conservatively serializes all pairs under that master.
    """

    if settings.l1_address:
        identity = f"l1:{settings.l1_address.casefold()}"
    else:
        identity = "accounts:" + ":".join(
            str(index) for index in sorted((settings.main.account_index, settings.sub.account_index))
        )
    return _neutral_lock_path(f"{settings.rest_url}|{settings.chain_id}|{identity}")


def _decimal(value: Any, default: str = "0") -> Decimal:
    if isinstance(value, Decimal):
        return value if value.is_finite() else Decimal(default)
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else Decimal(default)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _required_decimal(value: Any, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid {name}: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"invalid {name}: {value!r}")
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    # SDK response classes are usually pydantic/dataclass-like models. Convert
    # them before they reach the dashboard JSON encoder; unknown objects are
    # represented textually rather than taking the whole snapshot endpoint
    # down after an otherwise successful action.
    for method_name in ("to_dict", "model_dump", "dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                result = method()
            except TypeError:
                result = method(by_alias=True)
            if isinstance(result, Mapping):
                return _json_value(result)
    return str(value)


def _model_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    for method_name in ("to_dict", "model_dump", "dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                result = method()
            except TypeError:
                result = method(by_alias=True)
            if isinstance(result, Mapping):
                return dict(result)
    if value is None:
        return {}
    return {
        key: getattr(value, key)
        for key in dir(value)
        if not key.startswith("_") and not callable(getattr(value, key, None))
    }


def _first(mapping: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def _parse_private_keys(raw: Any, *, label: str, allow_empty: bool = True) -> Dict[int, str]:
    if raw is None or str(raw).strip() == "":
        if allow_empty:
            return {}
        raise ValueError(f"{label} is required for live actions")
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be a JSON object mapping API key index to private key") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"{label} must be a JSON object")
    result: Dict[int, str] = {}
    for raw_index, raw_key in payload.items():
        try:
            index = int(raw_index)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label} contains an invalid API key index {raw_index!r}") from exc
        if not 4 <= index <= 254:
            raise ValueError(f"{label} API key index {index} is outside the conservative RH range 4..254")
        key = str(raw_key or "").strip()
        if key[:2].casefold() == "0x":
            key = key[2:]
        if len(key) not in {64, 80} or any(char not in "0123456789abcdefABCDEF" for char in key):
            raise ValueError(f"{label}[{index}] must contain 64 or 80 hexadecimal characters")
        result[index] = key
    if not result and not allow_empty:
        raise ValueError(f"{label} contains no usable API keys")
    return result


@dataclass(frozen=True, slots=True)
class AccountSpec:
    name: str
    account_index: int
    api_key_index: int
    api_private_keys: Dict[int, str] = field(default_factory=dict)

    @property
    def has_write_credentials(self) -> bool:
        return bool(self.api_private_keys)


@dataclass(frozen=True, slots=True)
class LegSpec:
    account: str
    symbol: str
    market_id: int
    expected_sign: int


@dataclass(slots=True)
class NeutralSettings:
    main: AccountSpec
    sub: AccountSpec
    spy_market_id: int
    qqq_market_id: int
    # Symbol that is long on the master account. The subaccount is generated
    # as the exact opposite pair.
    main_long_symbol: str = "SPY"
    l1_address: str = ""
    poll_seconds: float = 5.0
    transfer_snapshot_max_age_seconds: float = 15.0
    transfer_recovery_successes_required: int = 3
    # Retained for configuration/backward compatibility.  Balance mode does
    # not use margin-ratio targets or an equity reserve to size transfers.
    min_margin_ratio: Decimal = Decimal("1.50")
    target_margin_ratio: Decimal = Decimal("2.00")
    reserve_usdc: Decimal = Decimal("50")
    transfer_hysteresis_usdc: Decimal = Decimal("10")
    max_transfer_usdc: Decimal = Decimal("1000")
    min_transfer_usdc: Decimal = Decimal("1")
    transfer_cooldown_seconds: float = 30.0
    close_slippage_bps: Decimal = Decimal("50")
    neutral_notional_tolerance: Decimal = Decimal("0.50")
    live: bool = False
    auto_transfer: bool = False
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8790
    dashboard_token: str = ""
    dashboard_username: str = "operator"
    dashboard_allow_public_bind: bool = False
    request_timeout_seconds: float = 10.0
    action_timeout_seconds: float = 20.0
    # A signer response is only an acceptance envelope.  Re-read account
    # state a bounded number of times before leaving the journal entry
    # blocked for operator reconciliation.
    confirmation_attempts: int = 3
    confirmation_poll_seconds: float = 0.5
    feishu_webhook_url: str = ""
    feishu_webhook_secret: str = ""
    feishu_report_interval_seconds: float = 600.0
    state_path: str = "logs/rh_neutral_manager_state.json"
    # These values are resolved together.  Keeping them on the settings
    # object prevents a read client and a signer from silently using different
    # Lighter deployments.
    rest_url: str = ROBINHOOD_MAINNET.rest_url
    ws_url: str = ROBINHOOD_MAINNET.ws_url
    chain_id: int = ROBINHOOD_MAINNET.chain_id

    @property
    def accounts(self) -> Tuple[AccountSpec, AccountSpec]:
        return self.main, self.sub

    @property
    def legs(self) -> Tuple[LegSpec, ...]:
        main_long_symbol = self.main_long_symbol.strip().upper()
        spy_sign = 1 if main_long_symbol == "SPY" else -1
        qqq_sign = -spy_sign
        return (
            LegSpec("main", "SPY", self.spy_market_id, spy_sign),
            LegSpec("main", "QQQ", self.qqq_market_id, qqq_sign),
            LegSpec("sub", "SPY", self.spy_market_id, -spy_sign),
            LegSpec("sub", "QQQ", self.qqq_market_id, -qqq_sign),
        )

    def validate(self, *, require_market_ids: bool = True) -> None:
        self.main_long_symbol = self.main_long_symbol.strip().upper()
        if self.main_long_symbol not in {"SPY", "QQQ"}:
            raise ValueError("main_long_symbol must be either SPY or QQQ")
        if self.main.name != "main" or self.sub.name != "sub":
            raise ValueError("account names must be exactly 'main' and 'sub'")
        if (self.main.account_index < 0 or self.sub.account_index < 0) and not self.l1_address:
            raise ValueError("main and sub account indexes must be configured (or provide RH_NEUTRAL_L1_ADDRESS)")
        if self.main.account_index >= 0 and self.sub.account_index >= 0 and self.main.account_index == self.sub.account_index:
            raise ValueError("main and sub account indexes must be different")
        for spec in self.accounts:
            if not 4 <= spec.api_key_index <= 254:
                raise ValueError(f"{spec.name} API key index must be in the conservative RH range 4..254")
            for key_index, private_key in spec.api_private_keys.items():
                if not 4 <= int(key_index) <= 254:
                    raise ValueError(f"{spec.name} private-key map contains reserved API key index {key_index}")
                normalized_key = str(private_key or "").strip()
                if normalized_key[:2].casefold() == "0x":
                    normalized_key = normalized_key[2:]
                if len(normalized_key) not in {64, 80} or any(
                    char not in "0123456789abcdefABCDEF" for char in normalized_key
                ):
                    raise ValueError(f"{spec.name} private-key map contains an invalid hexadecimal key")
        if self.main.api_key_index not in self.main.api_private_keys and self.live:
            raise ValueError("main API key index is missing from its private-key map")
        if self.sub.api_key_index not in self.sub.api_private_keys and self.live:
            raise ValueError("sub API key index is missing from its private-key map")
        if require_market_ids:
            if self.spy_market_id <= 0 or self.qqq_market_id <= 0:
                raise ValueError("SPY and QQQ market ids must be positive")
        elif self.spy_market_id < 0 or self.qqq_market_id < 0:
            raise ValueError("SPY and QQQ market ids must not be negative")
        if self.spy_market_id > 0 and self.qqq_market_id > 0 and self.spy_market_id == self.qqq_market_id:
            raise ValueError("SPY and QQQ market ids must be different")
        if not math.isfinite(self.poll_seconds) or self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        if (
            not math.isfinite(self.transfer_snapshot_max_age_seconds)
            or self.transfer_snapshot_max_age_seconds <= 0
        ):
            raise ValueError("transfer snapshot max age must be positive")
        if self.transfer_recovery_successes_required <= 0:
            raise ValueError("transfer recovery successes required must be positive")
        if self.min_margin_ratio <= 1:
            raise ValueError("min_margin_ratio must be greater than 1")
        if self.target_margin_ratio < self.min_margin_ratio:
            raise ValueError("target_margin_ratio must not be below min_margin_ratio")
        for name, value in (
            ("reserve_usdc", self.reserve_usdc),
            ("transfer_hysteresis_usdc", self.transfer_hysteresis_usdc),
            ("max_transfer_usdc", self.max_transfer_usdc),
            ("min_transfer_usdc", self.min_transfer_usdc),
            ("close_slippage_bps", self.close_slippage_bps),
            ("neutral_notional_tolerance", self.neutral_notional_tolerance),
        ):
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                raise ValueError(f"{name} must not be negative")
        if self.neutral_notional_tolerance >= 1:
            raise ValueError("neutral_notional_tolerance must be less than 1")
        if self.max_transfer_usdc <= 0:
            raise ValueError("max_transfer_usdc must be positive")
        if self.min_transfer_usdc <= 0 or self.min_transfer_usdc > self.max_transfer_usdc:
            raise ValueError("min_transfer_usdc must be positive and no larger than max_transfer_usdc")
        if (
            not math.isfinite(self.transfer_cooldown_seconds)
            or self.transfer_cooldown_seconds < 0
            or not math.isfinite(self.action_timeout_seconds)
            or self.action_timeout_seconds <= 0
        ):
            raise ValueError("transfer cooldown must be non-negative and action timeout must be positive")
        if self.confirmation_attempts <= 0:
            raise ValueError("confirmation_attempts must be positive")
        if not math.isfinite(self.confirmation_poll_seconds) or self.confirmation_poll_seconds < 0:
            raise ValueError("confirmation_poll_seconds must not be negative")
        if not math.isfinite(self.request_timeout_seconds) or self.request_timeout_seconds <= 0:
            raise ValueError("request timeout must be positive")
        if not math.isfinite(self.feishu_report_interval_seconds) or self.feishu_report_interval_seconds <= 0:
            raise ValueError("Feishu report interval must be positive")
        if self.feishu_webhook_url:
            parsed_feishu_url = urlparse(self.feishu_webhook_url)
            if parsed_feishu_url.scheme != "https" or not parsed_feishu_url.netloc:
                raise ValueError("Feishu webhook URL must use HTTPS")
        if self.dashboard_port < 0 or self.dashboard_port > 65535:
            raise ValueError("dashboard_port must be between 0 and 65535")
        if self.dashboard_host not in {"127.0.0.1", "::1"}:
            if not self.dashboard_allow_public_bind:
                raise ValueError("non-loopback dashboard binds require RH_NEUTRAL_DASHBOARD_ALLOW_PUBLIC=true")
            if not self.dashboard_token:
                raise ValueError("non-loopback dashboard binds require RH_NEUTRAL_DASHBOARD_TOKEN")
        if self.dashboard_token and len(self.dashboard_token) < 16:
            raise ValueError("dashboard token must contain at least 16 characters")
        if not self.dashboard_username.strip():
            raise ValueError("dashboard username must not be empty")
        if self.l1_address and (
            len(self.l1_address) != 42
            or not self.l1_address.lower().startswith("0x")
            or any(char not in "0123456789abcdefABCDEF" for char in self.l1_address[2:])
        ):
            raise ValueError("RH_NEUTRAL_L1_ADDRESS must be a 20-byte 0x-prefixed address")
        try:
            resolve_lighter_endpoint_profile(
                "robinhood",
                rest_url=self.rest_url,
                ws_url=self.ws_url,
                chain_id=self.chain_id,
            )
        except ValueError as exc:
            raise ValueError(
                "neutral manager must use the canonical Robinhood Lighter endpoint and signing chain"
            ) from exc

        # A true four-leg neutral layout needs the second account to carry the
        # opposite sign for each symbol.  Rejecting a duplicated pair here is
        # safer than silently monitoring a directional position.
        legs = {(leg.account, leg.symbol): leg.expected_sign for leg in self.legs}
        for symbol in ("SPY", "QQQ"):
            if legs[("main", symbol)] == legs[("sub", symbol)]:
                raise ValueError(
                    f"four-leg neutral layout requires opposite {symbol} signs on main and sub accounts"
                )


@dataclass(slots=True)
class PositionSnapshot:
    symbol: str
    market_id: int
    signed_size: Decimal
    position_value: Decimal
    avg_entry_price: Decimal
    unrealized_pnl: Decimal
    liquidation_price: Decimal
    initial_margin_fraction: Decimal
    allocated_margin: Decimal
    margin_mode: Optional[int] = None
    realized_pnl: Decimal = Decimal("0")

    @property
    def side(self) -> str:
        if self.signed_size > 0:
            return "long"
        if self.signed_size < 0:
            return "short"
        return "flat"

    def as_payload(self) -> Dict[str, Any]:
        return _json_value({
            "symbol": self.symbol,
            "market_id": self.market_id,
            "signed_size": self.signed_size,
            "side": self.side,
            "position_value": self.position_value,
            "avg_entry_price": self.avg_entry_price,
            "unrealized_pnl": self.unrealized_pnl,
            "liquidation_price": self.liquidation_price,
            "initial_margin_fraction": self.initial_margin_fraction,
            "allocated_margin": self.allocated_margin,
            "margin_mode": self.margin_mode,
            "realized_pnl": self.realized_pnl,
        })


@dataclass(slots=True)
class AccountSnapshot:
    name: str
    account_index: int
    l1_address: str
    equity: Decimal
    collateral: Decimal
    available_balance: Decimal
    initial_margin_requirement: Decimal
    maintenance_margin_requirement: Decimal
    pending_order_count: int
    transaction_time: int
    positions: List[PositionSnapshot]
    observed_at: float
    error: Optional[str] = None

    @property
    def maintenance_ratio(self) -> Optional[Decimal]:
        if self.maintenance_margin_requirement <= 0:
            return None
        return self.equity / self.maintenance_margin_requirement

    @property
    def maintenance_margin_usage_ratio(self) -> Optional[Decimal]:
        """Maintenance requirement as a fraction of account equity."""

        if self.equity <= 0:
            return None
        return self.maintenance_margin_requirement / self.equity

    @property
    def initial_ratio(self) -> Optional[Decimal]:
        if self.initial_margin_requirement <= 0:
            return None
        return self.equity / self.initial_margin_requirement

    @property
    def initial_margin_usage_ratio(self) -> Optional[Decimal]:
        """Initial requirement as a fraction of account equity."""

        if self.equity <= 0:
            return None
        return self.initial_margin_requirement / self.equity

    @property
    def maintenance_buffer(self) -> Decimal:
        return self.equity - self.maintenance_margin_requirement

    @property
    def has_isolated_positions(self) -> bool:
        return any(position.margin_mode == 1 and position.signed_size != 0 for position in self.positions)

    def position(self, symbol: str, market_id: int) -> Optional[PositionSnapshot]:
        # A market id is the authoritative identity.  Falling back to a
        # symbol after a positive id failed could select a similarly named
        # spot/index position and send a reduce-only order to the wrong leg.
        if market_id > 0:
            return next((item for item in self.positions if item.market_id == market_id), None)
        normalized = str(symbol or "").strip().upper()
        if normalized:
            return next((item for item in self.positions if item.symbol.upper() == normalized), None)
        return None

    def as_payload(self) -> Dict[str, Any]:
        return _json_value({
            "name": self.name,
            "account_index": self.account_index,
            "l1_address": self.l1_address,
            "equity": self.equity,
            "collateral": self.collateral,
            "available_balance": self.available_balance,
            "initial_margin_requirement": self.initial_margin_requirement,
            "maintenance_margin_requirement": self.maintenance_margin_requirement,
            "maintenance_ratio": self.maintenance_ratio,
            "maintenance_margin_usage_ratio": self.maintenance_margin_usage_ratio,
            "initial_ratio": self.initial_ratio,
            "initial_margin_usage_ratio": self.initial_margin_usage_ratio,
            "maintenance_buffer": self.maintenance_buffer,
            "has_isolated_positions": self.has_isolated_positions,
            "pending_order_count": self.pending_order_count,
            "transaction_time": self.transaction_time,
            "positions": [position.as_payload() for position in self.positions],
            "observed_at": self.observed_at,
            "error": self.error,
        })


@dataclass(frozen=True, slots=True)
class TransferPlan:
    source: str
    destination: str
    amount: Decimal
    reason: str
    urgent: bool = False

    def as_payload(self) -> Dict[str, Any]:
        return _json_value({
            "source": self.source,
            "destination": self.destination,
            "amount": self.amount,
            "reason": self.reason,
            "urgent": self.urgent,
        })


def margin_ratio(equity: Decimal, maintenance_requirement: Decimal) -> Optional[Decimal]:
    """Return equity / maintenance requirement, or None when no requirement exists."""

    return None if maintenance_requirement <= 0 else equity / maintenance_requirement


def build_transfer_plan(
    first: AccountSnapshot,
    second: AccountSnapshot,
    settings: NeutralSettings,
) -> Optional[TransferPlan]:
    """Build a plan that equalizes the two accounts' available balances.

    The target is the midpoint of the two fresh ``available_balance`` values.
    This intentionally does not use maintenance-margin targets: the manager is
    a simple collateral balancer for the already-opposite four-leg position.
    """

    if first.error or second.error:
        return None
    if not first.l1_address or not second.l1_address:
        return None
    if first.l1_address.casefold() != second.l1_address.casefold():
        return None
    # An account-level transfer does not add collateral to an isolated
    # position.  Refuse to infer safety in that mode; the operator must use a
    # dedicated update-margin action after reviewing the isolated leg.
    if first.has_isolated_positions or second.has_isolated_positions:
        return None
    if first.name == second.name:
        return None

    balance_delta = first.available_balance - second.available_balance
    if abs(balance_delta) <= settings.transfer_hysteresis_usdc:
        return None
    source_name = first.name if balance_delta > 0 else second.name
    recipient_name = second.name if balance_delta > 0 else first.name
    amount = min(abs(balance_delta) / Decimal("2"), settings.max_transfer_usdc)
    # USDG uses six decimal places.  Flooring avoids accidentally asking the
    # signer to transfer more than the source-side safety calculation allows.
    amount = amount.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
    if amount < getattr(settings, "min_transfer_usdc", Decimal("1")):
        return None
    reason = (
        f"available balance imbalance: {first.name}={first.available_balance} USDC, "
        f"{second.name}={second.available_balance} USDC"
    )
    return TransferPlan(source_name, recipient_name, amount, reason, False)


class LighterAccountGateway:
    """Public account reader plus lazy signer-backed write operations."""

    def __init__(
        self,
        spec: AccountSpec,
        settings: NeutralSettings,
        session: aiohttp.ClientSession,
    ) -> None:
        self.spec = spec
        self.settings = settings
        self.session = session
        self._signer: Any = None
        self._market_cache: Dict[int, Dict[str, Any]] = {}
        self._last_client_order_index = (1 << 47) + random.randint(1, 1 << 30)

    @property
    def name(self) -> str:
        return self.spec.name

    async def _get_json(
        self,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        *,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.settings.rest_url}{path}"
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout_seconds)
        async with self.session.get(url, params=params, headers=headers, timeout=timeout) as response:
            text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"Lighter {path} returned HTTP {response.status}: {text[:300]}")
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Lighter {path} returned invalid JSON") from exc
            if not isinstance(payload, dict):
                raise RuntimeError(f"Lighter {path} returned a non-object response")
            api_code = payload.get("code")
            if api_code not in (None, 0, 200, "0", "200"):
                raise RuntimeError(f"Lighter {path} returned API code {api_code}: {text[:300]}")
            return payload

    async def fetch_account(self) -> AccountSnapshot:
        payload = await self._get_json(
            "/api/v1/account",
            {"by": "index", "value": str(self.spec.account_index)},
        )
        accounts = payload.get("accounts")
        if not isinstance(accounts, list) or not accounts:
            raise RuntimeError(f"No account data returned for {self.spec.account_index}")
        raw = _model_dict(accounts[0])
        returned_index = _first(raw, "account_index", "accountIndex", "index")
        if self.settings.live and returned_index is None:
            raise RuntimeError(
                f"Lighter account response did not identify account {self.spec.account_index}"
            )
        if returned_index is not None:
            try:
                if int(returned_index) != self.spec.account_index:
                    raise RuntimeError(
                        f"Lighter returned account {returned_index} while querying {self.spec.account_index}"
                    )
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"Lighter returned an invalid account index: {returned_index!r}") from exc
        returned_l1 = str(_first(raw, "l1_address", "l1Address", default="") or "").strip()
        if self.settings.l1_address:
            if not returned_l1:
                raise RuntimeError(f"Lighter account {self.spec.account_index} did not return an L1 address")
            if returned_l1.casefold() != self.settings.l1_address.casefold():
                raise RuntimeError(
                    f"Lighter account {self.spec.account_index} is not under the configured L1 address"
                )
        if self.settings.live:
            raw_status = _first(raw, "status")
            raw_account_type = _first(raw, "account_type", "accountType")
            if raw_status is None or str(raw_status).strip().casefold() not in {
                "0", "1", "active", "enabled"
            }:
                raise RuntimeError(f"Lighter account {self.spec.account_index} has an unknown/inactive status")
            if raw_account_type is None:
                raise RuntimeError(f"Lighter account {self.spec.account_index} did not return account type")
            try:
                account_type = int(raw_account_type)
                if account_type not in TRADABLE_ACCOUNT_TYPES:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} is not a tradable main/sub account "
                        f"(account_type={account_type})"
                    )
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"Lighter account {self.spec.account_index} returned invalid account type") from exc
            required_account_fields = {
                "equity": ("cross_asset_value", "total_asset_value", "collateral"),
                "available_balance": ("available_balance", "availableBalance"),
                "maintenance_requirement": (
                    "cross_maintenance_margin_requirement",
                    "crossMaintenanceMarginRequirement",
                ),
            }
            for field_name, keys in required_account_fields.items():
                if not any(key in raw and raw[key] is not None for key in keys):
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} did not return {field_name}"
                    )

        def parse_account_decimal(
            value: Any,
            label: str,
            *,
            nonnegative: bool = False,
            default: Decimal = Decimal("0"),
        ) -> Decimal:
            if value is None:
                return default
            try:
                parsed = _required_decimal(value, label)
            except ValueError as exc:
                if self.settings.live:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} returned invalid {label}: {value!r}"
                    ) from exc
                return default
            if nonnegative and parsed < 0:
                if self.settings.live:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} returned negative {label}: {value!r}"
                    )
                return default
            return parsed

        def parse_account_int(value: Any, label: str, *, nonnegative: bool = False) -> int:
            if value is None:
                return 0
            try:
                parsed = int(str(value).strip())
            except (TypeError, ValueError) as exc:
                if self.settings.live:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} returned invalid {label}: {value!r}"
                    ) from exc
                return 0
            if nonnegative and parsed < 0:
                if self.settings.live:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} returned negative {label}: {value!r}"
                    )
                return 0
            return parsed

        positions: List[PositionSnapshot] = []
        seen_position_market_ids: set[int] = set()
        for raw_position in raw.get("positions") or []:
            item = _model_dict(raw_position)
            raw_market_id = _first(item, "market_id", "marketId", default=0)
            try:
                market_id = int(raw_market_id or 0)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Lighter account {self.spec.account_index} returned invalid position market id: {raw_market_id!r}"
                ) from exc
            if self.settings.live and market_id > 0:
                if market_id in seen_position_market_ids:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} returned duplicate position market id {market_id}"
                    )
                seen_position_market_ids.add(market_id)
            symbol = str(_first(item, "symbol", default=""))
            raw_size = _first(item, "position", default=None)
            raw_sign = _first(item, "sign")
            size = parse_account_decimal(
                raw_size,
                f"position size for market {market_id}",
                nonnegative=raw_sign is not None,
                default=Decimal("0"),
            )
            if raw_sign is None:
                signed_size = size
            else:
                try:
                    sign = int(str(raw_sign).strip())
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} returned invalid position sign: {raw_sign!r}"
                    ) from exc
                if self.settings.live and sign not in {-1, 1}:
                    raise RuntimeError(
                        f"Lighter account {self.spec.account_index} returned invalid position sign: {raw_sign!r}"
                    )
                signed_size = abs(size) * (1 if sign >= 0 else -1)
            raw_position_value = _first(item, "position_value", "positionValue")
            if self.settings.live and raw_position_value is None and signed_size != 0:
                raise RuntimeError(
                    f"Lighter account {self.spec.account_index} did not return position value for market {market_id}"
                )
            positions.append(PositionSnapshot(
                symbol=symbol,
                market_id=market_id,
                signed_size=signed_size,
                position_value=abs(parse_account_decimal(
                    raw_position_value,
                    f"position value for market {market_id}",
                    nonnegative=True,
                )),
                avg_entry_price=parse_account_decimal(
                    _first(item, "avg_entry_price", "avgEntryPrice"),
                    f"average entry price for market {market_id}",
                    nonnegative=True,
                ),
                unrealized_pnl=parse_account_decimal(
                    _first(item, "unrealized_pnl", "unrealizedPnl"),
                    f"unrealized PnL for market {market_id}",
                ),
                realized_pnl=parse_account_decimal(
                    _first(item, "realized_pnl", "realizedPnl"),
                    f"realized PnL for market {market_id}",
                ),
                liquidation_price=parse_account_decimal(
                    _first(item, "liquidation_price", "liquidationPrice"),
                    f"liquidation price for market {market_id}",
                    nonnegative=True,
                ),
                initial_margin_fraction=parse_account_decimal(
                    _first(item, "initial_margin_fraction", "initialMarginFraction"),
                    f"initial margin fraction for market {market_id}",
                    nonnegative=True,
                ),
                allocated_margin=parse_account_decimal(
                    _first(item, "allocated_margin", "allocatedMargin"),
                    f"allocated margin for market {market_id}",
                    nonnegative=True,
                ),
                margin_mode=(parse_account_int(
                    _first(item, "margin_mode", "marginMode"),
                    f"margin mode for market {market_id}",
                ) if _first(item, "margin_mode", "marginMode") is not None else None),
            ))
        raw_collateral = _first(raw, "collateral")
        collateral = parse_account_decimal(raw_collateral, "collateral", nonnegative=True)
        raw_equity = _first(raw, "cross_asset_value", "total_asset_value")
        equity = parse_account_decimal(
            collateral if raw_equity is None else raw_equity,
            "equity",
            nonnegative=True,
        )
        return AccountSnapshot(
            name=self.spec.name,
            account_index=self.spec.account_index,
            l1_address=returned_l1,
            equity=equity,
            collateral=collateral,
            available_balance=parse_account_decimal(
                _first(raw, "available_balance", "availableBalance"),
                "available balance",
                nonnegative=True,
            ),
            initial_margin_requirement=parse_account_decimal(
                _first(raw, "cross_initial_margin_requirement", "crossInitialMarginRequirement"),
                "initial margin requirement",
                nonnegative=True,
            ),
            maintenance_margin_requirement=parse_account_decimal(
                _first(raw, "cross_maintenance_margin_requirement", "crossMaintenanceMarginRequirement"),
                "maintenance margin requirement",
                nonnegative=True,
            ),
            pending_order_count=parse_account_int(
                _first(raw, "pending_order_count", "pendingOrderCount", default=0),
                "pending order count",
                nonnegative=True,
            ),
            transaction_time=parse_account_int(
                _first(raw, "transaction_time", "transactionTime", default=0),
                "transaction time",
                nonnegative=True,
            ),
            positions=positions,
            observed_at=time.time(),
        )

    async def discover_market_ids(self, symbols: Sequence[str]) -> Dict[str, int]:
        """Resolve market ids from the live perp catalogue.

        RWA market indexes are deployment data, not strategy constants.  The
        API has returned a few different wrapper names over time, so accept
        the documented ``order_books`` shape and equivalent legacy wrappers.
        """

        payload = await self._get_json("/api/v1/orderBooks", {"filter": "perp"})
        candidates: Any = payload.get("order_books") or payload.get("orderBooks")
        if candidates is None:
            candidates = payload.get("markets") or payload.get("data")
        if isinstance(candidates, Mapping):
            candidates = (
                candidates.get("order_books")
                or candidates.get("orderBooks")
                or list(candidates.values())
            )
        if not isinstance(candidates, (list, tuple)):
            candidates = []
        wanted = {str(symbol).strip().upper() for symbol in symbols}
        resolved: Dict[str, int] = {}
        for raw in candidates:
            item = _model_dict(raw)
            symbol = str(_first(item, "symbol", "ticker", "name", default="")).strip().upper()
            market_type = _first(item, "market_type", "marketType", "type")
            if market_type is not None and str(market_type).strip().casefold() not in {
                "perp", "perpetual", "perpetuals"
            }:
                continue
            market_status = _first(item, "status", "market_status", "marketStatus")
            if market_status is not None and str(market_status).strip().casefold() not in {
                "active", "1", "enabled", "open"
            }:
                continue
            market_id = _first(item, "market_id", "marketId", "index", "id")
            try:
                parsed_id = int(market_id)
            except (TypeError, ValueError):
                continue
            if symbol in wanted and parsed_id > 0:
                previous = resolved.get(symbol)
                if previous is not None and previous != parsed_id:
                    raise RuntimeError(
                        f"RH market catalogue returned multiple active perp ids for {symbol}: "
                        f"{previous} and {parsed_id}"
                    )
                resolved[symbol] = parsed_id
        missing = sorted(wanted - set(resolved))
        if missing:
            raise RuntimeError(
                "could not resolve RH Lighter perp market ids for: " + ", ".join(missing)
            )
        return resolved

    async def discover_account_indexes(self, l1_address: str, *, exclude: Optional[int] = None) -> Tuple[int, int]:
        """Return the first active master and a distinct active subaccount."""

        payload = await self._get_json(
            "/api/v1/accountsByL1Address",
            {"l1_address": str(l1_address).strip()},
        )
        # API revisions have used either `accounts` or `sub_accounts` (and
        # occasionally returned both). Merge all list-shaped fields so the
        # master is not lost when the subaccount list is non-empty.
        raw_accounts: List[Any] = []
        for key in ("accounts", "sub_accounts", "subAccounts"):
            value = payload.get(key)
            if isinstance(value, list):
                raw_accounts.extend(value)
        if not isinstance(raw_accounts, list):
            raise RuntimeError("accountsByL1Address returned no account list")
        candidates: List[Tuple[int, int]] = []
        for raw in raw_accounts:
            item = _model_dict(raw)
            try:
                index = int(_first(item, "index", "account_index", "accountIndex"))
            except (TypeError, ValueError):
                continue
            # RH has returned both numeric 0 (main) and 1 (subaccount) for
            # tradable accounts. Never fall back to reserved account types
            # just to get two indexes.
            status = _first(item, "status")
            if status is None:
                # Do not guess that an unlabelled account is active in live
                # mode; reserved/inactive entries have appeared in this list.
                continue
            if str(status).casefold() not in {"0", "1", "active", "enabled"}:
                continue
            raw_account_type = _first(item, "account_type", "accountType")
            if raw_account_type is None:
                continue
            try:
                account_type = int(raw_account_type)
            except (TypeError, ValueError):
                continue
            candidates.append((index, account_type))
        tradable = sorted({
            index for index, account_type in candidates
            if account_type in TRADABLE_ACCOUNT_TYPES
        })
        if len(tradable) < 2:
            raise RuntimeError(
                "fewer than two tradable active accounts found for the configured L1 address"
            )
        if len(tradable) > 2:
            raise RuntimeError(
                "more than two tradable active accounts found; configure RH_NEUTRAL_MAIN_ACCOUNT_INDEX "
                "and RH_NEUTRAL_SUB_ACCOUNT_INDEX explicitly"
            )
        if exclude is not None:
            alternatives = [index for index in tradable if index != exclude]
            if len(alternatives) < 1:
                raise RuntimeError("no distinct subaccount found for the configured main account")
            return exclude, alternatives[0]
        return tradable[0], tradable[1]

    async def fetch_market(self, market_id: int, *, force_refresh: bool = False) -> Dict[str, Any]:
        if force_refresh or market_id not in self._market_cache:
            self._market_cache[market_id] = await self._get_json(
                "/api/v1/orderBookDetails", {"market_id": market_id}
            )
        payload = self._market_cache[market_id]
        details = payload.get("order_book_details") or payload.get("orderBookDetails")
        if isinstance(details, list) and details:
            result = _model_dict(details[0])
        elif isinstance(details, Mapping):
            result = dict(details)
        else:
            # Some deployments return the detail object directly.
            result = payload
        if self.settings.live:
            def raw_values(*keys: str) -> List[Any]:
                return [
                    result[key]
                    for key in keys
                    if key in result
                    and result[key] is not None
                    and not (isinstance(result[key], str) and not result[key].strip())
                ]

            def parse_decimal_field(
                label: str,
                missing_message: str,
                *keys: str,
                integer: bool = False,
            ) -> Decimal:
                values = raw_values(*keys)
                if not values:
                    raise RuntimeError(f"market {market_id} did not return {missing_message}")
                parsed_values: List[Decimal] = []
                for raw in values:
                    try:
                        parsed = Decimal(str(raw).strip())
                    except (InvalidOperation, TypeError, ValueError) as exc:
                        raise RuntimeError(
                            f"market {market_id} returned invalid {label}: {raw!r}"
                        ) from exc
                    if not parsed.is_finite() or parsed < 0:
                        raise RuntimeError(
                            f"market {market_id} returned invalid {label}: {raw!r}"
                        )
                    if integer and parsed != parsed.to_integral_value():
                        raise RuntimeError(
                            f"market {market_id} returned non-integer {label}: {raw!r}"
                        )
                    parsed_values.append(parsed)
                # If an API wrapper exposes both legacy and supported fields,
                # never silently choose conflicting precision/limits.
                if any(value != parsed_values[0] for value in parsed_values[1:]):
                    raise RuntimeError(
                        f"market {market_id} returned conflicting {label} fields"
                    )
                return parsed_values[0]

            size_decimals = parse_decimal_field(
                "supported size precision",
                "supported size precision",
                "size_decimals",
                "supported_size_decimals",
                "sizeDecimals",
                "supportedSizeDecimals",
                integer=True,
            )
            price_decimals = parse_decimal_field(
                "supported price precision",
                "supported price precision",
                "price_decimals",
                "supported_price_decimals",
                "priceDecimals",
                "supportedPriceDecimals",
                integer=True,
            )
            if size_decimals > 18:
                raise RuntimeError(f"market {market_id} returned unsupported size precision {size_decimals}")
            if price_decimals > 18:
                raise RuntimeError(f"market {market_id} returned unsupported price precision {price_decimals}")
            parse_decimal_field(
                "minimum base amount",
                "minimum base amount",
                "min_base_amount",
                "minBaseAmount",
            )
            parse_decimal_field(
                "minimum quote amount",
                "minimum quote amount",
                "min_quote_amount",
                "minQuoteAmount",
            )
        return result

    async def refresh_market(self, market_id: int) -> Dict[str, Any]:
        """Fetch current market precision/limits before a live order."""

        # Evict first and call the one-argument public method so lightweight
        # test/read-only adapters that override `fetch_market` remain
        # compatible with live validation.
        self._market_cache.pop(int(market_id), None)
        return await self.fetch_market(market_id)

    async def validate_market_identity(self, market_id: int, expected_symbol: str) -> Dict[str, Any]:
        """Verify an id is the intended active RH perpetual market."""

        details = await self.fetch_market(market_id)
        returned_id = _first(details, "market_id", "marketId", "index", "id")
        if returned_id is not None:
            try:
                if int(returned_id) != int(market_id):
                    raise RuntimeError(
                        f"market endpoint returned id {returned_id!r} while querying {market_id}"
                    )
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"market {market_id} returned an invalid market id") from exc
        symbol = _first(details, "symbol", "ticker", "name")
        if symbol is None or str(symbol).strip().upper() != str(expected_symbol).strip().upper():
            raise RuntimeError(
                f"market id {market_id} identifies {symbol!r}, expected {expected_symbol.upper()}"
            )
        market_type = _first(details, "market_type", "marketType", "type")
        if self.settings.live and market_type is None:
            raise RuntimeError(f"market {market_id} did not return market type")
        if market_type is not None and str(market_type).strip().casefold() not in {
            "perp", "perpetual", "perpetuals"
        }:
            raise RuntimeError(f"market id {market_id} is not a perpetual market")
        status = _first(details, "status", "market_status", "marketStatus")
        if self.settings.live and status is None:
            raise RuntimeError(f"market {market_id} did not return market status")
        if status is not None and str(status).strip().casefold() not in {
            "active", "1", "enabled", "open"
        }:
            raise RuntimeError(f"market id {market_id} is not active (status={status!r})")
        return details

    async def fetch_bbo(self, market_id: int) -> Tuple[Decimal, Decimal]:
        payload = await self._get_json(
            "/api/v1/orderBookOrders", {"market_id": market_id, "limit": 20}
        )
        bids = payload.get("bids") or payload.get("buy") or []
        asks = payload.get("asks") or payload.get("sell") or []
        if not bids and not asks:
            # Some API revisions returned a mixed ``orders`` array.  Use the
            # explicit side rather than inferring it from price ordering.
            mixed = payload.get("orders") or payload.get("order_book_orders") or []
            if isinstance(mixed, list):
                bids, asks = [], []
                for raw in mixed:
                    item = _model_dict(raw)
                    side = _first(item, "side", "type")
                    is_ask = _first(item, "is_ask", "isAsk")
                    if is_ask is not None:
                        if isinstance(is_ask, bool):
                            parsed_is_ask = is_ask
                        else:
                            parsed_is_ask = str(is_ask).strip().casefold() in {"1", "true", "yes", "ask", "sell"}
                        side = "ask" if parsed_is_ask else "bid"
                    side_text = str(side or "").casefold()
                    if side_text in {"ask", "sell", "1", "true"}:
                        asks.append(raw)
                    elif side_text in {"bid", "buy", "0", "false"}:
                        bids.append(raw)

        def prices(levels: Any) -> List[Decimal]:
            result: List[Decimal] = []
            if not isinstance(levels, list):
                return result
            for level in levels:
                item = _model_dict(level)
                value = _first(item, "price", "px")
                if value is not None:
                    parsed = _decimal(value)
                    if parsed > 0:
                        result.append(parsed)
            return result

        bid_prices = prices(bids)
        ask_prices = prices(asks)
        if not bid_prices or not ask_prices:
            raise RuntimeError(f"No two-sided BBO for market {market_id}")
        best_bid, best_ask = max(bid_prices), min(ask_prices)
        if best_bid >= best_ask:
            raise RuntimeError(f"Crossed or invalid BBO for market {market_id}: {best_bid} >= {best_ask}")
        return best_bid, best_ask

    def _get_signer(self) -> Any:
        if self._signer is not None:
            return self._signer
        if SignerClient is None:
            raise RuntimeError("lighter SDK is not installed; live actions are unavailable")
        if not self.spec.api_private_keys:
            raise RuntimeError(f"No private API key configured for {self.name}")
        self._signer = SignerClient(
            url=self.settings.rest_url,
            account_index=self.spec.account_index,
            api_private_keys=dict(self.spec.api_private_keys),
            chain_id=self.settings.chain_id,
        )
        return self._signer

    def _reserve_client_order_index(self) -> int:
        current = max(int(time.time_ns() // 1_000_000), self._last_client_order_index + 1)
        if current >= (1 << 48):
            raise RuntimeError("Lighter client order index exhausted uint48 range")
        self._last_client_order_index = current
        return current

    async def _auth_token(self) -> str:
        signer = self._get_signer()
        token, error = signer.create_auth_token_with_expiry(
            getattr(signer, "DEFAULT_10_MIN_AUTH_EXPIRY", -1),
            api_key_index=self.spec.api_key_index,
        )
        if error:
            raise RuntimeError(f"Unable to create Lighter auth token: {error}")
        return str(token)

    @staticmethod
    def _transfer_memo(reason: str) -> str:
        """Encode a human reason into the SDK's fixed 32-byte memo format."""

        return "0x" + hashlib.sha256(str(reason).encode("utf-8")).hexdigest()

    @staticmethod
    def _signed_result_failed(result: Any) -> bool:
        """Whether a signer result is an explicit, non-ambiguous rejection."""

        if not isinstance(result, (tuple, list)):
            return False
        if len(result) >= 3 and result[2]:
            return True
        if len(result) < 2:
            return False
        response = result[1]
        code = getattr(response, "code", None)
        if isinstance(response, Mapping):
            code = response.get("code", code)
        return code not in (None, 0, 200, "0", "200")

    async def _call_signed(self, method: Any, **kwargs: Any) -> Any:
        """Call one explicit-key SDK method with its nonce manager held.

        The SDK decorator bypasses its internal lock when ``api_key_index`` is
        supplied.  We therefore allocate the nonce and serialize calls here.
        Unknown transport exceptions deliberately do not roll the nonce back:
        the sequencer may have received the transaction, so reusing it is less
        safe than leaving a gap.
        """

        signer = self._get_signer()
        key_index = self.spec.api_key_index
        nonce_manager = getattr(signer, "nonce_manager", None)
        if nonce_manager is None or not hasattr(nonce_manager, "async_next_nonce"):
            return await method(**kwargs, api_key_index=key_index)
        lock = nonce_manager.lock(key_index)
        async with lock:
            _, nonce = await nonce_manager.async_next_nonce(key_index)
            result = await method(**kwargs, nonce=nonce, api_key_index=key_index)
            if self._signed_result_failed(result):
                nonce_manager.acknowledge_failure(key_index)
            return result

    async def transfer_same_master(self, destination: int, amount: Decimal, memo: str) -> Dict[str, Any]:
        if not self.settings.live:
            raise RuntimeError("live actions are disabled; refusing direct transfer call")
        if int(destination) == self.spec.account_index:
            raise ValueError("transfer destination must differ from the source account")
        if amount <= 0:
            raise ValueError("transfer amount must be positive")
        configured_indexes = {spec.account_index for spec in self.settings.accounts if spec.account_index >= 0}
        if int(destination) not in configured_indexes:
            raise ValueError("transfer destination is not one of the configured accounts")
        if amount < self.settings.min_transfer_usdc:
            raise ValueError("transfer amount is below the configured minimum")
        if amount > self.settings.max_transfer_usdc:
            raise ValueError("transfer amount exceeds the configured maximum")
        signer = self._get_signer()
        fee_raw = DEFAULT_TRANSFER_FEE_RAW
        # Query the dynamic fee instead of guessing.  Live mode fails closed
        # if the endpoint cannot be authenticated or read; only dry-run keeps
        # a zero placeholder for preview output.
        try:
            fee_payload = await self._get_json(
                "/api/v1/transferFeeInfo",
                {"account_index": self.spec.account_index, "to_account_index": destination},
                headers={"Authorization": await self._auth_token()},
            )
            fee_value = _first(fee_payload, "transfer_fee_usdc", "transferFeeUsdc")
            if fee_value is None and isinstance(fee_payload.get("data"), Mapping):
                fee_value = _first(fee_payload["data"], "transfer_fee_usdc", "transferFeeUsdc")
            fee_raw = int(fee_value or 0)
        except Exception as exc:
            if self.settings.live:
                raise RuntimeError(
                    f"Could not query transfer fee for {self.name} -> {destination}; refusing to transfer"
                ) from exc
            LOGGER.warning(
                "Could not query transfer fee for %s -> %s: %s; using zero fee in dry-run",
                self.name,
                destination,
                exc,
            )
        try:
            result = await self._call_signed(
                signer.transfer_same_master_account,
                to_account_index=int(destination),
                asset_id=USDC_ASSET_ID,
                route_from=ROUTE_PERPS,
                route_to=ROUTE_PERPS,
                amount=float(amount.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)),
                fee=fee_raw,
                memo=self._transfer_memo(memo),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise LighterWriteUncertainError(
                f"transfer submission status is unknown for {self.name} -> {destination}",
                metadata={"source": self.name, "destination": int(destination), "amount": amount},
            ) from exc
        uncertainty_metadata = {
            "source": self.name,
            "destination": int(destination),
            "amount": amount,
        }
        if not isinstance(result, (tuple, list)) or len(result) < 3:
            raise LighterWriteUncertainError(
                "Lighter transfer returned an unrecognised response; status is unknown",
                metadata=uncertainty_metadata,
            )
        tx_info, response, error = result[0], result[1], result[2]
        if error:
            raise LighterWriteUncertainError(
                f"Lighter transfer returned an error; status is unknown: {error}",
                metadata=uncertainty_metadata,
            )
        response_code = getattr(response, "code", None)
        if isinstance(response, Mapping):
            response_code = response.get("code", response_code)
        if response_code not in (200, "200"):
            raise LighterWriteUncertainError(
                f"Lighter transfer returned unexpected response code {response_code!r}",
                metadata=uncertainty_metadata,
            )
        tx_hash = getattr(response, "tx_hash", None)
        if isinstance(response, Mapping):
            tx_hash = response.get("tx_hash", response.get("txHash", tx_hash))
        try:
            serialized_transfer = {
                "status": "accepted_pending_confirmation",
                "tx": _json_value(tx_info),
                "tx_hash": _json_value(tx_hash),
                "response": _json_value(response),
                "fee_raw": fee_raw,
            }
        except Exception as exc:
            # The signer already returned, so a response-model serialization
            # failure is itself an unknown post-submit outcome.
            raise LighterWriteUncertainError(
                "Lighter transfer response could not be serialized; status is unknown",
                metadata=uncertainty_metadata,
            ) from exc
        return serialized_transfer

    async def close_position(
        self,
        market_id: int,
        *,
        quantity: Optional[Decimal],
        slippage_bps: Decimal,
        dry_run: bool,
    ) -> Dict[str, Any]:
        if not dry_run and not self.settings.live:
            raise RuntimeError("live actions are disabled; refusing direct close call")
        snapshot = await self.fetch_account()
        position = snapshot.position("", market_id)
        signed_size = position.signed_size if position else Decimal("0")
        if signed_size == 0:
            return {"status": "already_flat", "account": self.name, "market_id": market_id}
        requested = abs(signed_size) if quantity is None else _required_decimal(quantity, "quantity")
        if requested <= 0 or requested > abs(signed_size):
            raise ValueError(f"close quantity must be in (0, {abs(signed_size)}]")
        # Market precision/minimums are deployment data and may change while
        # the process is running. A live close must validate them against a
        # fresh detail response immediately before signing.
        if not dry_run:
            # Refresh and re-validate the market identity immediately before
            # signing.  A listing/index change after startup must not turn a
            # reduce-only request into an order for a different instrument.
            await self.refresh_market(market_id)
            expected_symbol = None
            if int(market_id) == int(self.settings.spy_market_id):
                expected_symbol = "SPY"
            elif int(market_id) == int(self.settings.qqq_market_id):
                expected_symbol = "QQQ"
            details = (
                await self.validate_market_identity(market_id, expected_symbol)
                if expected_symbol
                else await self.fetch_market(market_id)
            )
        else:
            details = await self.fetch_market(market_id)
        size_decimals = int(_first(details, "size_decimals", "supported_size_decimals", default=5) or 5)
        price_decimals = int(_first(details, "price_decimals", "supported_price_decimals", default=1) or 1)
        size_step = Decimal(1).scaleb(-size_decimals)
        price_step = Decimal(1).scaleb(-price_decimals)
        normalized_qty = requested.quantize(size_step, rounding=ROUND_DOWN)
        if normalized_qty <= 0:
            raise ValueError("close quantity is below the market size step")
        bid, ask = await self.fetch_bbo(market_id)
        slippage = max(Decimal("0"), slippage_bps) / Decimal("10000")
        is_ask = signed_size > 0
        raw_price = bid * (Decimal("1") - slippage) if is_ask else ask * (Decimal("1") + slippage)
        price = raw_price.quantize(price_step, rounding=ROUND_DOWN if is_ask else ROUND_UP)
        min_base = _decimal(_first(details, "min_base_amount", "minBaseAmount", default="0"))
        min_quote = _decimal(_first(details, "min_quote_amount", "minQuoteAmount", default="0"))
        full_close = normalized_qty >= abs(signed_size) - size_step
        if min_base > 0 and normalized_qty < min_base and not full_close:
            raise ValueError(f"close quantity {normalized_qty} is below market minimum {min_base}")
        if min_quote > 0 and normalized_qty * price < min_quote and not full_close:
            raise ValueError(
                f"close notional {normalized_qty * price} is below market minimum {min_quote}"
            )
        plan = {
            "status": "dry_run" if dry_run else "accepted_pending_confirmation",
            "account": self.name,
            "market_id": market_id,
            # Keep the exact pre-submit state in the non-secret result.  The
            # account endpoint is the confirmation source after an IOC write;
            # without this value a later position reduction cannot be tied to
            # this particular request.
            "pre_close_signed_size": signed_size,
            "pre_close_transaction_time": snapshot.transaction_time,
            "side": "sell" if is_ask else "buy",
            "quantity": normalized_qty,
            "price": price,
            "reduce_only": True,
            "time_in_force": "ioc",
            "fill_confirmation_required": not dry_run,
        }
        if dry_run:
            return _json_value(plan)
        signer = self._get_signer()
        base_amount = int((normalized_qty * (Decimal(10) ** size_decimals)).to_integral_value(rounding=ROUND_DOWN))
        integer_price = int((price * (Decimal(10) ** price_decimals)).to_integral_value(rounding=ROUND_DOWN))
        client_order_index = self._reserve_client_order_index()
        try:
            create_order, response, error = await self._call_signed(
                signer.create_order,
                market_index=int(market_id),
                client_order_index=client_order_index,
                base_amount=base_amount,
                price=integer_price,
                is_ask=is_ask,
                order_type=getattr(signer, "ORDER_TYPE_LIMIT", 0),
                time_in_force=getattr(signer, "ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", 0),
                reduce_only=True,
                trigger_price=0,
                order_expiry=getattr(signer, "DEFAULT_IOC_EXPIRY", 0),
                self_trade_behavior_mode=getattr(signer, "SELF_TRADE_BEHAVIOR_EXPIRE_BOTH", 2),
                self_trade_equality_mode=getattr(signer, "SELF_TRADE_EQUALITY_MASTER_ACCOUNT_INDEX", 1),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise LighterWriteUncertainError(
                f"close order submission status is unknown for {self.name} market {market_id}",
                metadata={
                    "account": self.name,
                    "market_id": int(market_id),
                    "client_order_index": client_order_index,
                    "quantity": normalized_qty,
                },
            ) from exc
        uncertainty_metadata = {
            "account": self.name,
            "market_id": int(market_id),
            "client_order_index": client_order_index,
            "quantity": normalized_qty,
        }
        if error:
            raise LighterWriteUncertainError(
                f"Lighter close order returned an error; status is unknown: {error}",
                metadata=uncertainty_metadata,
            )
        response_code = getattr(response, "code", None)
        if isinstance(response, Mapping):
            response_code = response.get("code", response_code)
        if response_code not in (200, "200"):
            raise LighterWriteUncertainError(
                f"Lighter close order returned code {response_code}; status is unknown",
                metadata=uncertainty_metadata,
            )
        tx_hash = getattr(response, "tx_hash", None)
        if isinstance(response, Mapping):
            tx_hash = response.get("tx_hash", response.get("txHash", tx_hash))
        try:
            plan.update({
                "client_order_index": client_order_index,
                "tx_hash": _json_value(tx_hash),
                "response": _json_value(response),
            })
            return _json_value(plan)
        except Exception as exc:
            # Do not let a local response encoding error turn an accepted
            # signer call into a retryable/deterministically acknowledged
            # result.
            raise LighterWriteUncertainError(
                "Lighter close response could not be serialized; status is unknown",
                metadata=uncertainty_metadata,
            ) from exc

    async def close(self) -> None:
        signer = self._signer
        if signer is not None:
            with contextlib.suppress(Exception):
                await signer.close()
            self._signer = None


class NeutralPositionManager:
    """Coordinate monitoring, risk transfers, and manual reduce-only closes."""

    def __init__(self, settings: NeutralSettings) -> None:
        settings.validate(require_market_ids=False)
        self.settings = settings
        self._state_path = Path(settings.state_path or "logs/rh_neutral_manager_state.json").expanduser()
        if not self._state_path.is_absolute():
            self._state_path = Path.cwd() / self._state_path
        self._instance_lock = _NeutralInstanceLockGroup(_neutral_identity_lock_paths(settings))
        self._session: Optional[aiohttp.ClientSession] = None
        self.gateways: Dict[str, LighterAccountGateway] = {}
        self.snapshots: Dict[str, AccountSnapshot] = {}
        self.last_plan: Optional[TransferPlan] = None
        self.last_transfer: Optional[Dict[str, Any]] = None
        self.action_history: List[Dict[str, Any]] = []
        self._action_lock = asyncio.Lock()
        self._seen_action_ids: Dict[str, float] = {}
        self._stop_event = asyncio.Event()
        self._dashboard: Any = None
        self._pair_error: Optional[str] = None
        self._last_refresh_error: Optional[str] = None
        self._transfer_block_reason: Optional[str] = None
        self._transfer_recovery_successes = 0
        self._next_feishu_report_at = 0.0
        self._pending_unknown_records: List[Dict[str, Any]] = []
        self._pending_tasks: Dict[str, asyncio.Future[Any]] = {}
        self._load_state()

    def _load_state(self) -> None:
        """Restore only non-secret action metadata; pending writes stay blocked."""

        try:
            if not self._state_path.exists():
                return
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping):
                raise ValueError("state root must be an object")
            history = payload.get("action_history")
            if isinstance(history, list):
                self.action_history = [dict(item) for item in history[-100:] if isinstance(item, Mapping)]
            transfer = payload.get("last_transfer")
            if isinstance(transfer, Mapping):
                self.last_transfer = dict(transfer)
            seen = payload.get("seen_action_ids")
            if isinstance(seen, Mapping):
                self._seen_action_ids = {
                    str(key): float(value)
                    for key, value in seen.items()
                    if isinstance(value, (int, float))
                }
            pending = payload.get("pending_unknown")
            if isinstance(pending, list):
                self._pending_unknown_records = [dict(item) for item in pending if isinstance(item, Mapping)]
        except Exception as exc:
            # A corrupt journal is itself a reason to refuse live writes. The
            # monitor can still start read-only so an operator can inspect it.
            LOGGER.error("Unable to load neutral manager state %s: %s", self._state_path, exc)
            self._pending_unknown_records = [{"status": "unknown_journal", "error": str(exc)}]

    def _persist_state(self) -> None:
        payload = {
            "version": 1,
            "updated_at": time.time(),
            "action_history": self.action_history[-100:],
            "last_transfer": self.last_transfer,
            "seen_action_ids": self._seen_action_ids,
            "pending_unknown": self._pending_unknown_records,
        }
        path = self._state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
        temp.write_text(json.dumps(_json_value(payload), ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        with contextlib.suppress(OSError):
            os.chmod(temp, 0o600)
        os.replace(temp, path)

    def _persist_state_checked(self) -> None:
        """Persist journal state, refusing live writes if storage is broken."""

        try:
            self._persist_state()
        except Exception as exc:
            if self.settings.live:
                raise NeutralJournalError(
                    f"neutral manager journal is unavailable at {self._state_path}; refusing live action"
                ) from exc
            LOGGER.error("Unable to persist neutral manager state %s: %s", self._state_path, exc)

    def _pending_write_reason(self) -> Optional[str]:
        # A timed-out SDK task may complete locally after the HTTP deadline,
        # but that still does not prove the sequencer accepted/filled it.  Keep
        # every such record blocking until an operator reconciles the exchange
        # state and explicitly marks the journal entry acknowledged.
        active = [
            item for item in self._pending_unknown_records
            if item.get("status") not in {"reconciled", "acknowledged"}
        ]
        if active:
            return "a previous write has unknown exchange status; reconcile it before new live actions"
        if self._pending_tasks:
            return "a timed-out write is still running; wait for reconciliation before new live actions"
        return None

    def _mark_journal_uncertain_in_memory(self, pending_id: str, error: str) -> None:
        """Keep a write blocked when the journal itself becomes unavailable.

        This helper intentionally does not call `_persist_state_checked` again:
        the failure may be a full disk or permissions outage.  The existing
        intent record remains on disk (it was written before the signer call),
        while the in-memory status prevents another action in this process.
        """

        record = next(
            (item for item in self._pending_unknown_records if item.get("pending_id") == pending_id),
            None,
        )
        if record is not None:
            record["status"] = "unknown_journal"
            record["error"] = str(error)

    def _finish_pending_task(self, pending_id: str, task: asyncio.Future[Any]) -> None:
        self._pending_tasks.pop(pending_id, None)
        for record in self._pending_unknown_records:
            if record.get("pending_id") != pending_id or record.get("status") != "unknown_pending":
                continue
            try:
                result = task.result()
            except asyncio.CancelledError:
                record.update({"status": "cancelled_after_timeout"})
            except Exception as exc:
                record.update({"status": "failed_after_timeout", "error": str(exc)})
            else:
                record.update({"status": "completed_after_timeout", "result": _json_value(result)})
            break
        # A completed/failed timeout is still part of the safety journal.  If
        # persistence fails, keep the in-memory block and log loudly; the
        # caller must not be allowed to start another live action.
        try:
            self._persist_state()
        except Exception:
            LOGGER.critical("Unable to persist pending neutral write state %s", self._state_path, exc_info=True)

    def _begin_write_intent(self, label: str, metadata: Mapping[str, Any]) -> str:
        intent_id = uuid.uuid4().hex
        record = {
            **_json_value(dict(metadata)),
            "pending_id": intent_id,
            "label": label,
            "status": "intent",
            "created_at": time.time(),
        }
        self._pending_unknown_records.append(record)
        self._persist_state_checked()
        return intent_id

    def _update_pending_record(
        self,
        pending_id: str,
        *,
        status: str,
        result: Any = None,
        error: Optional[str] = None,
    ) -> None:
        record = next(
            (item for item in self._pending_unknown_records if item.get("pending_id") == pending_id),
            None,
        )
        if record is None:
            raise RuntimeError(f"missing journal record for pending write {pending_id}")
        previous = dict(record)
        record["status"] = status
        if result is not None:
            record["result"] = _json_value(result)
        if error:
            record["error"] = str(error)
        try:
            self._persist_state_checked()
        except Exception:
            # Keep an active blocking status in memory when the journal cannot
            # be durably updated.  In particular, never turn an accepted
            # pending write into ``acknowledged`` only in RAM.
            record.clear()
            record.update(previous)
            raise

    def _mark_pending_write(
        self,
        label: str,
        task: asyncio.Future[Any],
        metadata: Mapping[str, Any],
        *,
        pending_id: Optional[str] = None,
    ) -> str:
        pending_id = pending_id or self._begin_write_intent(label, metadata)
        record = next(
            (item for item in self._pending_unknown_records if item.get("pending_id") == pending_id),
            None,
        )
        if record is None:
            raise RuntimeError(f"missing journal record for pending write {pending_id}")
        record.update({**_json_value(dict(metadata)), "label": label, "status": "unknown_pending"})
        self._pending_tasks[pending_id] = task
        task.add_done_callback(lambda completed: self._finish_pending_task(pending_id, completed))
        self._persist_state_checked()
        return pending_id

    def _mark_pending_result(
        self,
        label: str,
        result: Any,
        metadata: Mapping[str, Any],
        *,
        status: str = "accepted_pending_confirmation",
        pending_id: Optional[str] = None,
    ) -> str:
        """Persist a successful API acceptance until final state is checked."""

        pending_id = pending_id or self._begin_write_intent(label, metadata)
        record = next(
            (item for item in self._pending_unknown_records if item.get("pending_id") == pending_id),
            None,
        )
        if record is None:
            raise RuntimeError(f"missing journal record for pending write {pending_id}")
        previous = dict(record)
        record.update({
            **_json_value(dict(metadata)),
            "label": label,
            "status": status,
            "result": _json_value(result),
        })
        try:
            self._persist_state_checked()
        except Exception:
            record.clear()
            record.update(previous)
            raise
        return pending_id

    @staticmethod
    def _unconfirmed_status(value: Any) -> Optional[str]:
        statuses = {
            "unknown_pending",
            "unknown_journal",
            "accepted_pending_confirmation",
            "completed_after_timeout",
            "failed_after_timeout",
            "cancelled_after_timeout",
        }
        if isinstance(value, Mapping):
            status = str(value.get("status", "")).strip().casefold()
            if status in statuses:
                return status
            for child in value.values():
                found = NeutralPositionManager._unconfirmed_status(child)
                if found:
                    return found
        elif isinstance(value, (list, tuple)):
            for child in value:
                found = NeutralPositionManager._unconfirmed_status(child)
                if found:
                    return found
        return None

    @staticmethod
    def _pending_ids(value: Any) -> List[str]:
        """Collect pending journal ids from an action result tree."""

        found: List[str] = []

        def visit(item: Any) -> None:
            if isinstance(item, Mapping):
                pending_id = item.get("pending_id")
                if pending_id is not None:
                    value = str(pending_id).strip()
                    if value and value not in found:
                        found.append(value)
                for child in item.values():
                    visit(child)
            elif isinstance(item, (list, tuple)):
                for child in item:
                    visit(child)

        visit(value)
        return found

    @staticmethod
    def _replace_pending_result(value: Any, pending_id: str, confirmation: Mapping[str, Any]) -> Any:
        """Return a result tree with one accepted pending child acknowledged."""

        if isinstance(value, Mapping):
            updated = {
                key: NeutralPositionManager._replace_pending_result(child, pending_id, confirmation)
                for key, child in value.items()
            }
            if str(value.get("pending_id", "")) == pending_id:
                updated["status"] = "acknowledged"
                updated["confirmation"] = _json_value(dict(confirmation))
            return updated
        if isinstance(value, list):
            return [NeutralPositionManager._replace_pending_result(child, pending_id, confirmation) for child in value]
        if isinstance(value, tuple):
            return tuple(NeutralPositionManager._replace_pending_result(child, pending_id, confirmation) for child in value)
        return value

    @staticmethod
    def _snapshot_balances(snapshot: Optional[AccountSnapshot]) -> Dict[str, Any]:
        if snapshot is None:
            return {}
        return _json_value({
            "equity": snapshot.equity,
            "collateral": snapshot.collateral,
            "available_balance": snapshot.available_balance,
            "transaction_time": snapshot.transaction_time,
        })

    def _transfer_confirmation_metadata(self, source: str, destination: str, amount: Decimal) -> Dict[str, Any]:
        """Capture balances immediately before a same-master transfer."""

        return {
            "kind": "transfer",
            "source": source,
            "destination": destination,
            "amount": amount,
            "before": {
                source: self._snapshot_balances(self.snapshots.get(source)),
                destination: self._snapshot_balances(self.snapshots.get(destination)),
            },
        }

    def _close_confirmation_metadata(
        self,
        account: str,
        symbol: str,
        market_id: int,
        quantity: Optional[Decimal],
    ) -> Dict[str, Any]:
        """Capture the observed position context for a reduce-only write."""

        snapshot = self.snapshots.get(account)
        position = snapshot.position(symbol, market_id) if snapshot else None
        return {
            "kind": "close",
            "account": account,
            "symbol": symbol.upper(),
            "market_id": int(market_id),
            "requested_quantity": quantity,
            "before_signed_size": position.signed_size if position else None,
            "before_transaction_time": snapshot.transaction_time if snapshot else 0,
        }

    @staticmethod
    def _decimal_from_mapping(mapping: Mapping[str, Any], key: str) -> Optional[Decimal]:
        if key not in mapping or mapping.get(key) is None:
            return None
        try:
            value = Decimal(str(mapping[key]))
        except (InvalidOperation, TypeError, ValueError):
            return None
        return value if value.is_finite() else None

    def _confirm_transfer_record(self, record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        source_name = str(record.get("source", ""))
        destination_name = str(record.get("destination", ""))
        amount = _decimal(record.get("amount"))
        before = record.get("before")
        if not source_name or not destination_name or amount <= 0 or not isinstance(before, Mapping):
            return None
        source_before = before.get(source_name)
        destination_before = before.get(destination_name)
        source_after = self.snapshots.get(source_name)
        destination_after = self.snapshots.get(destination_name)
        if (
            not isinstance(source_before, Mapping)
            or not isinstance(destination_before, Mapping)
            or source_after is None
            or destination_after is None
            or source_after.error
            or destination_after.error
        ):
            return None

        # A transfer should move collateral in opposite directions.  Check
        # each balance representation independently because API deployments
        # have differed in which of equity/collateral/available is updated
        # first.  Requiring both legs and a near-amount delta prevents a
        # normal market PnL tick from acknowledging a write accidentally.
        tolerance = max(Decimal("0.000010"), amount * Decimal("0.02"))
        upper_tolerance = max(Decimal("0.000010"), amount * Decimal("0.25"))
        before_transaction = {
            source_name: self._decimal_from_mapping(source_before, "transaction_time"),
            destination_name: self._decimal_from_mapping(destination_before, "transaction_time"),
        }
        for field_name in ("collateral", "equity", "available_balance"):
            source_old = self._decimal_from_mapping(source_before, field_name)
            destination_old = self._decimal_from_mapping(destination_before, field_name)
            if source_old is None or destination_old is None:
                continue
            source_new = getattr(source_after, field_name)
            destination_new = getattr(destination_after, field_name)
            source_delta = source_old - source_new
            destination_delta = destination_new - destination_old
            if (
                source_delta + tolerance < amount
                or destination_delta + tolerance < amount
                or source_delta > amount + upper_tolerance
                or destination_delta > amount + upper_tolerance
            ):
                continue
            # A non-zero transaction clock is required for automatic
            # acknowledgement.  If a deployment omits it (or returns zero),
            # leave the accepted write blocked for manual reconciliation
            # rather than treating an unrelated balance move as proof.
            source_clock = before_transaction[source_name]
            destination_clock = before_transaction[destination_name]
            clocks = [
                (source_clock, source_after.transaction_time),
                (destination_clock, destination_after.transaction_time),
            ]
            clocks = [
                (old, new) for old, new in clocks
                if old not in (None, Decimal("0"))
            ]
            if not clocks or not any(int(new) > int(old) for old, new in clocks):
                continue
            return {
                "method": "account_state",
                "kind": "transfer",
                "field": field_name,
                "amount": amount,
                "source_delta": source_delta,
                "destination_delta": destination_delta,
                "observed_at": time.time(),
            }
        return None

    def _confirm_close_record(self, record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        account = str(record.get("account", ""))
        symbol = str(record.get("symbol", "")).upper()
        try:
            market_id = int(record.get("market_id"))
        except (TypeError, ValueError):
            return None
        snapshot = self.snapshots.get(account)
        if snapshot is None or snapshot.error:
            return None
        position = snapshot.position(symbol, market_id)
        after_size = abs(position.signed_size) if position else Decimal("0")
        result = record.get("result")
        # The gateway includes this exact value in its accepted response; use
        # it in preference to a possibly stale manager snapshot. Metadata is
        # the fallback for API revisions that omit the echo field.
        before_signed = (
            self._decimal_from_mapping(result, "pre_close_signed_size")
            if isinstance(result, Mapping)
            else None
        )
        if before_signed is None:
            before_signed = self._decimal_from_mapping(record, "before_signed_size")
        before_clock = (
            self._decimal_from_mapping(result, "pre_close_transaction_time")
            if isinstance(result, Mapping)
            else None
        )
        if before_clock is None:
            before_clock = self._decimal_from_mapping(record, "before_transaction_time")
        if before_signed is None or before_signed == 0:
            return None
        if before_clock in (None, Decimal("0")):
            return None
        if snapshot.transaction_time <= int(before_clock):
            return None
        before_size = abs(before_signed)
        reduction = before_size - after_size
        # A reduce-only IOC can fill partially.  Any positive reduction is
        # authoritative evidence that the accepted request changed state;
        # the residual remains visible in the dashboard for a later operator
        # action.  A non-positive delta means the exchange has not confirmed
        # this write yet.
        if reduction <= Decimal("0"):
            return None
        requested = (
            self._decimal_from_mapping(record, "requested_quantity")
            if record.get("requested_quantity") is not None
            else None
        )
        if requested is None and isinstance(result, Mapping):
            requested = self._decimal_from_mapping(result, "quantity")
        if requested is not None and requested > 0:
            close_tolerance = max(Decimal("0.00000001"), requested * Decimal("0.02"))
            if reduction > requested + close_tolerance:
                return None
        elif reduction > before_size:
            return None
        if position is not None and position.signed_size != 0:
            if before_signed is not None and before_signed * position.signed_size < 0:
                return None
        return {
            "method": "account_state",
            "kind": "close",
            "account": account,
            "symbol": symbol,
            "market_id": market_id,
            "before_size": before_size,
            "after_size": after_size,
            "reduction": reduction,
            "observed_at": time.time(),
        }

    def _confirm_pending_record(self, record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        if str(record.get("status", "")).casefold() != "accepted_pending_confirmation":
            return None
        kind = str(record.get("kind", "")).casefold()
        if kind == "transfer" or (record.get("source") is not None and record.get("destination") is not None):
            return self._confirm_transfer_record(record)
        if kind == "close" or record.get("market_id") is not None:
            return self._confirm_close_record(record)
        return None

    async def _reconcile_pending_records(self, pending_ids: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """Acknowledge only accepted writes proven by fresh account state."""

        wanted = {str(item) for item in pending_ids} if pending_ids is not None else None
        confirmations: Dict[str, Any] = {}
        for record in self._pending_unknown_records:
            pending_id = str(record.get("pending_id", ""))
            if not pending_id or (wanted is not None and pending_id not in wanted):
                continue
            confirmation = self._confirm_pending_record(record)
            if confirmation is None:
                continue
            self._update_pending_record(
                pending_id,
                status="acknowledged",
                result={
                    **(record.get("result") if isinstance(record.get("result"), Mapping) else {}),
                    "status": "acknowledged",
                    "confirmation": confirmation,
                },
            )
            confirmations[pending_id] = confirmation
        return confirmations

    async def _write_with_timeout(
        self,
        label: str,
        awaitable: Any,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        intent_id = self._begin_write_intent(label, metadata or {})
        try:
            task = asyncio.ensure_future(awaitable)
        except Exception as exc:
            self._update_pending_record(intent_id, status="acknowledged", error=str(exc))
            raise
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=self.settings.action_timeout_seconds)
            status = self._unconfirmed_status(result)
            if status is not None:
                pending_id = self._mark_pending_result(
                    label, result, metadata or {}, status=status, pending_id=intent_id
                )
                if isinstance(result, Mapping):
                    result = {**result, "pending_id": pending_id}
                else:
                    result = {"status": status, "pending_id": pending_id, "result": _json_value(result)}
            else:
                self._update_pending_record(intent_id, status="acknowledged", result=result)
            return result
        except NeutralJournalError as exc:
            # The signer may already have accepted the write, but the intent
            # transition could not be persisted.  Keep the in-memory record
            # blocked and surface an explicit unknown-journal result; never
            # downgrade this path to deterministic ``acknowledged``.
            self._mark_journal_uncertain_in_memory(intent_id, str(exc))
            LOGGER.critical("%s completed with an unavailable write journal", label, exc_info=True)
            return {"status": "unknown_journal", "pending_id": intent_id, "label": label}
        except asyncio.TimeoutError:
            try:
                pending_id = self._mark_pending_write(label, task, metadata or {}, pending_id=intent_id)
            except NeutralJournalError as exc:
                self._mark_journal_uncertain_in_memory(intent_id, str(exc))
                LOGGER.critical("%s timed out and its journal could not be updated", label, exc_info=True)
                return {"status": "unknown_journal", "pending_id": intent_id, "label": label}
            LOGGER.error("%s timed out; exchange status is unknown (pending_id=%s)", label, pending_id)
            return {"status": "unknown_pending", "pending_id": pending_id, "label": label}
        except LighterWriteUncertainError as exc:
            metadata_with_error = dict(metadata or {})
            metadata_with_error.update(exc.metadata)
            metadata_with_error["error"] = str(exc)
            try:
                pending_id = self._mark_pending_result(
                    label,
                    {"status": "unknown_pending", "error": str(exc)},
                    metadata_with_error,
                    status="unknown_pending",
                    pending_id=intent_id,
                )
            except NeutralJournalError as journal_exc:
                self._mark_journal_uncertain_in_memory(intent_id, str(journal_exc))
                LOGGER.critical("%s failed and its journal could not be updated", label, exc_info=True)
                return {"status": "unknown_journal", "pending_id": intent_id, "label": label}
            LOGGER.error("%s failed with unknown exchange status (pending_id=%s)", label, pending_id)
            return {"status": "unknown_pending", "pending_id": pending_id, "label": label}
        except (aiohttp.ClientError, ConnectionError, OSError) as exc:
            # A transport failure can happen after the sequencer accepted the
            # request.  Keep the operation blocked rather than blindly retrying.
            try:
                pending_id = self._mark_pending_result(
                    label,
                    {"status": "unknown_pending", "error": str(exc)},
                    {**(metadata or {}), "error": str(exc)},
                    status="unknown_pending",
                    pending_id=intent_id,
                )
            except NeutralJournalError as journal_exc:
                self._mark_journal_uncertain_in_memory(intent_id, str(journal_exc))
                LOGGER.critical("%s transport failed and its journal could not be updated", label, exc_info=True)
                return {"status": "unknown_journal", "pending_id": intent_id, "label": label}
            LOGGER.error("%s failed with transport uncertainty (pending_id=%s)", label, pending_id)
            return {"status": "unknown_pending", "pending_id": pending_id, "label": label}
        except asyncio.CancelledError:
            try:
                pending_id = self._mark_pending_write(label, task, metadata or {}, pending_id=intent_id)
            except NeutralJournalError as exc:
                self._mark_journal_uncertain_in_memory(intent_id, str(exc))
                LOGGER.critical("%s was cancelled and its journal could not be updated", label, exc_info=True)
                raise
            LOGGER.error("%s was cancelled while exchange status was unknown (pending_id=%s)", label, pending_id)
            raise
        except Exception as exc:
            # The pinned SDK exposes HTTP failures as ServiceException/
            # ApiException rather than aiohttp.ClientError.  Keep those in the
            # same unknown-status bucket; deterministic validation errors still
            # propagate normally.
            if exc.__class__.__name__ not in {"ServiceException", "ApiException", "ApiError"}:
                try:
                    self._update_pending_record(intent_id, status="acknowledged", error=str(exc))
                except NeutralJournalError as journal_exc:
                    self._mark_journal_uncertain_in_memory(intent_id, str(journal_exc))
                    LOGGER.critical("%s failed and its journal could not be updated", label, exc_info=True)
                raise
            try:
                pending_id = self._mark_pending_result(
                    label,
                    {"status": "unknown_pending", "error": str(exc)},
                    {**(metadata or {}), "error": str(exc)},
                    status="unknown_pending",
                    pending_id=intent_id,
                )
            except NeutralJournalError as journal_exc:
                self._mark_journal_uncertain_in_memory(intent_id, str(journal_exc))
                LOGGER.critical("%s SDK call failed and its journal could not be updated", label, exc_info=True)
                return {"status": "unknown_journal", "pending_id": intent_id, "label": label}
            LOGGER.error("%s failed with SDK uncertainty (pending_id=%s)", label, pending_id)
            return {"status": "unknown_pending", "pending_id": pending_id, "label": label}

    async def start(self) -> None:
        if self._session is not None or self._dashboard is not None:
            return
        self._stop_event.clear()
        try:
            await self._start_impl()
        except Exception:
            # Do not leave an aiohttp session or a partially initialized signer
            # alive when market discovery or the initial read fails.
            await self.stop()
            raise

    async def _start_impl(self) -> None:
        # Validate account indexes and endpoint first, while allowing market
        # ids to be discovered from the live catalogue below.
        self.settings.validate(require_market_ids=False)
        self._instance_lock.acquire()
        self._persist_state()
        self._session = aiohttp.ClientSession()
        if self.settings.l1_address and (
            self.settings.main.account_index < 0 or self.settings.sub.account_index < 0
        ):
            discovery_spec = AccountSpec(
                "discovery",
                max(0, self.settings.main.account_index),
                self.settings.main.api_key_index,
                self.settings.main.api_private_keys,
            )
            discovery = LighterAccountGateway(discovery_spec, self.settings, self._session)
            if self.settings.main.account_index < 0 and self.settings.sub.account_index >= 0:
                _, main_index = await discovery.discover_account_indexes(
                    self.settings.l1_address,
                    exclude=self.settings.sub.account_index,
                )
                sub_index = self.settings.sub.account_index
            else:
                main_index, sub_index = await discovery.discover_account_indexes(
                    self.settings.l1_address,
                    exclude=self.settings.main.account_index if self.settings.main.account_index >= 0 else None,
                )
            if self.settings.main.account_index < 0:
                self.settings.main = AccountSpec(
                    self.settings.main.name,
                    main_index,
                    self.settings.main.api_key_index,
                    self.settings.main.api_private_keys,
                )
            if self.settings.sub.account_index < 0:
                self.settings.sub = AccountSpec(
                    self.settings.sub.name,
                    sub_index,
                    self.settings.sub.api_key_index,
                    self.settings.sub.api_private_keys,
                )
            # Discovery filled in explicit indexes after the initial L1 lock
            # was acquired.  Add the per-account aliases before constructing
            # gateways or issuing any write-capable operation.
            self._instance_lock.add_and_acquire(_neutral_identity_lock_paths(self.settings))
        self.gateways = {
            spec.name: LighterAccountGateway(spec, self.settings, self._session)
            for spec in self.settings.accounts
        }
        await self._resolve_market_ids()
        self.settings.validate()
        await self.refresh_once()
        self._validate_master_pair()
        from strategies.neutral_dashboard import NeutralDashboard

        self._dashboard = NeutralDashboard(
            self.snapshot_payload,
            self._handle_dashboard_action,
            host=self.settings.dashboard_host,
            port=self.settings.dashboard_port,
            username=self.settings.dashboard_username if self.settings.dashboard_token else None,
            password=self.settings.dashboard_token or None,
            allow_public_bind=self.settings.dashboard_allow_public_bind,
            allowed_accounts=tuple(spec.name for spec in self.settings.accounts),
            allowed_symbols=("SPY", "QQQ"),
        )
        await self._dashboard.start()
        LOGGER.info(
            "Neutral manager ready: main=%s sub=%s SPY=%s QQQ=%s live=%s auto_transfer=%s dashboard=%s:%s",
            self.settings.main.account_index,
            self.settings.sub.account_index,
            self.settings.spy_market_id,
            self.settings.qqq_market_id,
            self.settings.live,
            self.settings.auto_transfer,
            self.settings.dashboard_host,
            self._dashboard.bound_port,
        )

    async def _resolve_market_ids(self) -> None:
        gateway = self.gateways.get("main") or next(iter(self.gateways.values()))
        if self.settings.spy_market_id <= 0 or self.settings.qqq_market_id <= 0:
            resolved = await gateway.discover_market_ids(("SPY", "QQQ"))
            if self.settings.spy_market_id <= 0:
                self.settings.spy_market_id = resolved["SPY"]
            if self.settings.qqq_market_id <= 0:
                self.settings.qqq_market_id = resolved["QQQ"]
        if self.settings.spy_market_id == self.settings.qqq_market_id:
            raise RuntimeError("SPY and QQQ resolved to the same market id")
        # Even explicitly configured ids must be checked against the live
        # catalogue.  This prevents a stale id from making flatten_all target
        # an unrelated market after a listing/index change.
        await gateway.validate_market_identity(self.settings.spy_market_id, "SPY")
        await gateway.validate_market_identity(self.settings.qqq_market_id, "QQQ")

    def _validate_master_pair(self) -> None:
        first = self.snapshots.get("main")
        second = self.snapshots.get("sub")
        if not first or not second or first.error or second.error:
            self._pair_error = "both accounts must be readable before transfers"
            return
        if not first.l1_address or not second.l1_address:
            self._pair_error = "L1 master address is missing from one account"
            return
        if first.l1_address.casefold() != second.l1_address.casefold():
            self._pair_error = "main and sub accounts do not share the same L1 master address"
            return
        self._pair_error = None

    async def refresh_once(self) -> Dict[str, Any]:
        if not self.gateways:
            if self._session is None:
                self._session = aiohttp.ClientSession()
            self.gateways = {
                spec.name: LighterAccountGateway(spec, self.settings, self._session)
                for spec in self.settings.accounts
            }
        results = await asyncio.gather(
            *(gateway.fetch_account() for gateway in self.gateways.values()),
            return_exceptions=True,
        )
        self._last_refresh_error = None
        for name, result in zip(self.gateways, results):
            # `asyncio.CancelledError` is a BaseException on supported Python
            # versions.  Never turn task cancellation into a fake account
            # snapshot: propagate it so shutdown remains prompt.  Other
            # BaseException values are retained as stale-read errors rather
            # than being treated as successful account payloads.
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                old = self.snapshots.get(name)
                if old is not None:
                    old.error = str(result)
                    self.snapshots[name] = old
                else:
                    spec = self.gateways[name].spec
                    self.snapshots[name] = AccountSnapshot(
                        name=name, account_index=spec.account_index, l1_address="",
                        equity=Decimal("0"), collateral=Decimal("0"),
                        available_balance=Decimal("0"), initial_margin_requirement=Decimal("0"),
                        maintenance_margin_requirement=Decimal("0"), pending_order_count=0,
                        transaction_time=0, positions=[], observed_at=time.time(), error=str(result),
                    )
                self._last_refresh_error = str(result)
            else:
                self.snapshots[name] = result
        self._validate_master_pair()
        snapshots_ok = (
            len(self.snapshots) == len(self.settings.accounts)
            and not self._last_refresh_error
            and not self._pair_error
            and all(not snapshot.error for snapshot in self.snapshots.values())
        )
        self._transfer_recovery_successes = (
            min(self._transfer_recovery_successes + 1, self.settings.transfer_recovery_successes_required)
            if snapshots_ok else 0
        )
        return self.snapshot_payload()

    def _transfer_health(self) -> Dict[str, Any]:
        """Return the fail-closed transfer circuit state and its reason."""

        required = self.settings.transfer_recovery_successes_required
        ages: Dict[str, Optional[float]] = {}
        for name in ("main", "sub"):
            snapshot = self.snapshots.get(name)
            if snapshot is None:
                ages[name] = None
            else:
                ages[name] = max(0.0, time.time() - float(snapshot.observed_at))
        if self._pending_write_reason():
            reason = self._pending_write_reason() or "unconfirmed write is blocking transfers"
            state = "blocked"
        elif self._last_refresh_error:
            reason = f"account refresh failed: {self._last_refresh_error}"
            state = "blocked"
        elif len(self.snapshots) != len(self.settings.accounts):
            reason = "both account snapshots are required before transfers"
            state = "blocked"
        elif any(snapshot.error for snapshot in self.snapshots.values()):
            failed = next((snapshot for snapshot in self.snapshots.values() if snapshot.error), None)
            reason = f"account snapshot failed: {failed.name if failed else 'unknown'}: {failed.error if failed else 'unknown'}"
            state = "blocked"
        elif self._pair_error:
            reason = self._pair_error
            state = "blocked"
        elif self.settings.live and any(
            age is None or age > self.settings.transfer_snapshot_max_age_seconds for age in ages.values()
        ):
            stale = [
                f"{name}={age:.1f}s" if age is not None else f"{name}=missing"
                for name, age in ages.items()
                if age is None or age > self.settings.transfer_snapshot_max_age_seconds
            ]
            reason = (
                "account snapshot is stale; transfers blocked "
                f"({', '.join(stale)}, max={self.settings.transfer_snapshot_max_age_seconds:.1f}s)"
            )
            state = "blocked"
        elif self.settings.live and self._transfer_recovery_successes < required:
            reason = (
                "waiting for consecutive healthy account snapshots before transfers "
                f"({self._transfer_recovery_successes}/{required})"
            )
            state = "recovering"
        elif not self.settings.live:
            reason = "live mode is disabled; transfers are read-only"
            state = "read_only"
        else:
            reason = None
            state = "ready"
        return {
            "state": state,
            "reason": reason,
            "allowed": state == "ready",
            "snapshot_ages": ages,
            "recovery_successes": self._transfer_recovery_successes,
            "recovery_required": required,
        }

    def _ensure_transfer_write(self) -> None:
        self._ensure_live_write()
        health = self._transfer_health()
        if health["state"] != "ready":
            raise RuntimeError(str(health["reason"] or "transfers are currently blocked"))

    def _ensure_live_write(self) -> None:
        if not self.settings.live:
            raise RuntimeError("live actions are disabled; start with --live after validating dry-run output")
        if self._pair_error:
            raise RuntimeError(self._pair_error)
        pending_reason = self._pending_write_reason()
        if pending_reason:
            raise RuntimeError(pending_reason)

    def _ensure_transfer_cooldown(self) -> None:
        if not self.last_transfer:
            return
        try:
            elapsed = time.time() - float(self.last_transfer.get("timestamp", 0))
        except (TypeError, ValueError):
            raise RuntimeError("last transfer timestamp is invalid; reconcile the state journal")
        remaining = self.settings.transfer_cooldown_seconds - elapsed
        if remaining > 0:
            raise RuntimeError(
                f"transfer cooldown is active for another {remaining:.1f}s; do not retry yet"
            )

    def _neutral_layout_report(self) -> Dict[str, Any]:
        """Return a conservative four-leg direction/notional health report."""

        report: Dict[str, Any] = {
            "ready": True,
            "reason": None,
            "tolerance": self.settings.neutral_notional_tolerance,
            "symbols": {},
        }
        expected = {(leg.account, leg.symbol): leg for leg in self.settings.legs}
        for leg in self.settings.legs:
            snapshot = self.snapshots.get(leg.account)
            position = snapshot.position(leg.symbol, leg.market_id) if snapshot else None
            if position is None or position.signed_size == 0:
                report["ready"] = False
                report["reason"] = f"four-leg layout incomplete: {leg.account} {leg.symbol} is flat or missing"
                return report
            if position.signed_size * expected[(leg.account, leg.symbol)].expected_sign <= 0:
                report["ready"] = False
                report["reason"] = f"{leg.account} {leg.symbol} sign is opposite to the configured neutral layout"
                return report

        for symbol in ("SPY", "QQQ"):
            main = self.snapshots["main"].position(symbol, self._market_for_symbol(symbol))
            sub = self.snapshots["sub"].position(symbol, self._market_for_symbol(symbol))
            main_value = abs(main.position_value) if main else Decimal("0")
            sub_value = abs(sub.position_value) if sub else Decimal("0")
            # A few API revisions omit position_value; use signed size * entry
            # price only as a conservative fallback for the warning calculation.
            if main_value <= 0 and main:
                main_value = abs(main.signed_size * main.avg_entry_price)
            if sub_value <= 0 and sub:
                sub_value = abs(sub.signed_size * sub.avg_entry_price)
            largest = max(main_value, sub_value)
            skew = (abs(main_value - sub_value) / largest) if largest > 0 else None
            symbol_payload = {
                "main_notional": main_value,
                "sub_notional": sub_value,
                "skew_ratio": skew,
                "within_tolerance": bool(skew is not None and skew <= self.settings.neutral_notional_tolerance),
            }
            report["symbols"][symbol] = symbol_payload
            if skew is None:
                report["ready"] = False
                report["reason"] = f"{symbol} notional is unavailable for one or both legs"
                break
            if skew > self.settings.neutral_notional_tolerance:
                report["ready"] = False
                report["reason"] = (
                    f"{symbol} cross-account notional skew {skew:.4f} exceeds "
                    f"tolerance {self.settings.neutral_notional_tolerance:.4f}"
                )
                break
        return report

    def _claim_action_id(self, request_id: Optional[str]) -> str:
        value = str(request_id or uuid.uuid4().hex).strip()
        if not value or len(value) > 128:
            raise ValueError("request_id must be a non-empty string no longer than 128 characters")
        now = time.time()
        self._seen_action_ids = {
            key: timestamp for key, timestamp in self._seen_action_ids.items()
            if now - timestamp < 3600
        }
        if value in self._seen_action_ids:
            raise ValueError("request_id has already been used")
        if len(self._seen_action_ids) >= MAX_ACTION_ID_CACHE:
            oldest = min(self._seen_action_ids, key=self._seen_action_ids.get)
            self._seen_action_ids.pop(oldest, None)
        self._seen_action_ids[value] = now
        self._persist_state_checked()
        return value

    async def calculate_transfer_plan(self) -> Optional[TransferPlan]:
        self.last_plan = None
        health = self._transfer_health()
        if health["state"] in {"blocked", "recovering"}:
            self._transfer_block_reason = str(health["reason"] or "transfers are currently blocked")
            return None
        first = self.snapshots.get("main")
        second = self.snapshots.get("sub")
        if not first or not second:
            self._transfer_block_reason = "both account snapshots are required"
            return None
        if first.error or second.error:
            self._transfer_block_reason = "transfer paused while an account snapshot is stale or failed"
            return None
        if self._pair_error:
            self._transfer_block_reason = self._pair_error
            return None
        pending_reason = self._pending_write_reason()
        if pending_reason:
            self._transfer_block_reason = pending_reason
            return None
        if first.has_isolated_positions or second.has_isolated_positions:
            self._transfer_block_reason = "isolated positions require explicit update-margin handling"
            return None
        positions: Dict[Tuple[str, str], Optional[PositionSnapshot]] = {}
        for leg in self.settings.legs:
            snapshot = self.snapshots.get(leg.account)
            positions[(leg.account, leg.symbol)] = (
                snapshot.position(leg.symbol, leg.market_id) if snapshot else None
            )
            position = positions[(leg.account, leg.symbol)]
            if position is not None and position.signed_size != 0 and position.signed_size * leg.expected_sign < 0:
                self._transfer_block_reason = (
                    f"{leg.account} {leg.symbol} sign is opposite to the configured neutral layout"
                )
                return None
        for leg in self.settings.legs:
            position = positions[(leg.account, leg.symbol)]
            if position is None or position.signed_size == 0:
                self._transfer_block_reason = (
                    f"four-leg layout incomplete: {leg.account} {leg.symbol} position is missing or flat"
                )
                return None
        layout_report = self._neutral_layout_report()
        if not layout_report["ready"]:
            self._transfer_block_reason = str(layout_report["reason"])
            return None
        self.last_plan = build_transfer_plan(first, second, self.settings)
        self._transfer_block_reason = None if self.last_plan else "accounts are within transfer thresholds"
        return self.last_plan

    async def execute_transfer(self, plan: TransferPlan, *, request_id: Optional[str] = None) -> Dict[str, Any]:
        async with self._action_lock:
            self._ensure_live_write()
            self._ensure_transfer_cooldown()
            action_id = self._claim_action_id(request_id)
            await self.refresh_once()
            self._ensure_transfer_write()
            fresh_plan = await self.calculate_transfer_plan()
            if fresh_plan is None:
                return {"status": "balanced", "plan": None}
            plan = fresh_plan
            gateway = self.gateways.get(plan.source)
            destination = self.gateways.get(plan.destination)
            if gateway is None or destination is None:
                raise ValueError("transfer plan references an unknown account")
            result = await self._write_with_timeout(
                f"transfer:{action_id}",
                gateway.transfer_same_master(destination.spec.account_index, plan.amount, plan.reason),
                self._transfer_confirmation_metadata(plan.source, plan.destination, plan.amount),
            )
            record = {"type": "transfer", "plan": plan.as_payload(), "result": result, "timestamp": time.time()}
            self.last_transfer = record
            self._record_action(record)
            await self._reconcile_after_write(record)
            return _json_value(record)

    async def manual_rebalance(self, *, request_id: Optional[str] = None) -> Dict[str, Any]:
        async with self._action_lock:
            if self.settings.live:
                self._ensure_live_write()
                self._ensure_transfer_cooldown()
            action_id = self._claim_action_id(request_id)
            await self.refresh_once()
            if self.settings.live:
                self._ensure_transfer_write()
            plan = await self.calculate_transfer_plan()
            if plan is None:
                return {"status": "balanced", "plan": None}
            if not self.settings.live:
                return {"status": "dry_run", "plan": plan.as_payload()}
            gateway = self.gateways[plan.source]
            destination = self.gateways[plan.destination]
            result = await self._write_with_timeout(
                f"transfer:{action_id}",
                gateway.transfer_same_master(destination.spec.account_index, plan.amount, plan.reason),
                self._transfer_confirmation_metadata(plan.source, plan.destination, plan.amount),
            )
            record = {"type": "transfer", "plan": plan.as_payload(), "result": result, "timestamp": time.time()}
            self.last_transfer = record
            self._record_action(record)
            await self._reconcile_after_write(record)
            return _json_value(record)

    async def _handle_dashboard_action(self, action: Any) -> Dict[str, Any]:
        """Translate the dashboard's validated action into manager methods."""

        action_name = getattr(action, "action", "")
        request_id = getattr(action, "request_id", None)
        if action_name == "rebalance":
            return await self.manual_rebalance(request_id=request_id)
        if action_name == "close_position":
            quantity = getattr(action, "quantity", None)
            fraction = getattr(action, "fraction", None)
            if quantity is None and fraction is not None:
                await self.refresh_once()
                snapshot = self.snapshots.get(str(action.account))
                position = snapshot.position(str(action.symbol), self._market_for_symbol(str(action.symbol))) if snapshot else None
                if position is None or position.signed_size == 0:
                    return {"status": "already_flat", "account": action.account, "symbol": action.symbol}
                quantity = abs(position.signed_size) * fraction
            return await self.close_one(
                str(action.account),
                str(action.symbol),
                quantity,
                request_id=request_id,
            )
        if action_name == "close_pair":
            quantities: Dict[str, Any] = {}
            if getattr(action, "quantity", None) is None and getattr(action, "fraction", None) is not None:
                await self.refresh_once()
            # Use both configured gateways, not only accounts whose last
            # polling cycle succeeded.  An explicit quantity should still be
            # sent to both accounts; the gateway re-reads and validates the
            # live position immediately before submitting a reduce-only order.
            for name in self.gateways:
                snapshot = self.snapshots.get(name)
                position = snapshot.position(str(action.symbol), self._market_for_symbol(str(action.symbol))) if snapshot else None
                if getattr(action, "quantity", None) is not None:
                    quantities[name] = action.quantity
                elif getattr(action, "fraction", None) is not None and position is not None:
                    quantities[name] = abs(position.signed_size) * action.fraction
                elif getattr(action, "fraction", None) is not None:
                    raise RuntimeError(
                        f"cannot calculate fractional close for {name}: account snapshot unavailable"
                    )
            return await self.close_both(str(action.symbol), quantities, request_id=request_id)
        if action_name == "flatten_all":
            return await self.flatten_all(request_id=request_id)
        raise ValueError(f"unsupported dashboard action {action_name!r}")

    async def close_one(
        self,
        account: str,
        symbol: str,
        quantity: Optional[Decimal],
        *,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        async with self._action_lock:
            self._ensure_live_write()
            action_id = self._claim_action_id(request_id)
            market_id = self._market_for_symbol(symbol)
            gateway = self.gateways.get(account)
            if gateway is None:
                raise ValueError(f"unknown account {account!r}")
            result = await self._write_with_timeout(
                f"close:{action_id}",
                gateway.close_position(
                    market_id,
                    quantity=quantity,
                    slippage_bps=self.settings.close_slippage_bps,
                    dry_run=False,
                ),
                self._close_confirmation_metadata(account, symbol, market_id, quantity),
            )
            record = {"type": "close", "result": result, "timestamp": time.time()}
            self._record_action(record)
            await self._reconcile_after_write(record)
            return _json_value(record)

    async def close_both(
        self,
        symbol: str,
        quantities: Optional[Mapping[str, Any]] = None,
        *,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        async with self._action_lock:
            self._ensure_live_write()
            action_id = self._claim_action_id(request_id)
            market_id = self._market_for_symbol(symbol)
            quantities = quantities or {}
            operations = []
            for name, gateway in self.gateways.items():
                raw_quantity = quantities.get(name)
                parsed_quantity = None if raw_quantity in (None, "") else _required_decimal(raw_quantity, "quantity")
                operations.append(self._write_with_timeout(
                    f"close_pair:{action_id}:{name}",
                    gateway.close_position(
                        market_id,
                        quantity=parsed_quantity,
                        slippage_bps=self.settings.close_slippage_bps,
                        dry_run=False,
                    ),
                    self._close_confirmation_metadata(name, symbol, market_id, parsed_quantity),
                ))
            results = await asyncio.gather(*operations, return_exceptions=True)
            payload = {
                name: ({"error": str(result)} if isinstance(result, Exception) else result)
                for name, result in zip(self.gateways, results)
            }
            record = {"type": "close_both", "symbol": symbol.upper(), "results": payload, "timestamp": time.time()}
            self._record_action(record)
            await self._reconcile_after_write(record)
            return _json_value(record)

    async def flatten_all(self, *, request_id: Optional[str] = None) -> Dict[str, Any]:
        """Close both configured symbols on both accounts, reduce-only.

        The four requests use independent account signers and are concurrent,
        but the exchange cannot make them atomic.  The returned per-leg result
        is therefore the source of truth for any follow-up retry.
        """

        async with self._action_lock:
            self._ensure_live_write()
            action_id = self._claim_action_id(request_id)
            operations = []
            labels = []
            for name, gateway in self.gateways.items():
                for symbol, market_id in (("SPY", self.settings.spy_market_id), ("QQQ", self.settings.qqq_market_id)):
                    labels.append((name, symbol))
                    operations.append(self._write_with_timeout(
                        f"flatten:{action_id}:{name}:{symbol}",
                        gateway.close_position(
                            market_id,
                            quantity=None,
                            slippage_bps=self.settings.close_slippage_bps,
                            dry_run=False,
                        ),
                        self._close_confirmation_metadata(name, symbol, market_id, None),
                    ))
            results = await asyncio.gather(*operations, return_exceptions=True)
            payload: Dict[str, Any] = {}
            for (name, symbol), result in zip(labels, results):
                payload[f"{name}:{symbol}"] = {"error": str(result)} if isinstance(result, Exception) else result
            record = {"type": "flatten_all", "results": payload, "timestamp": time.time()}
            self._record_action(record)
            await self._reconcile_after_write(record)
            return _json_value(record)

    def _market_for_symbol(self, symbol: str) -> int:
        normalized = str(symbol or "").strip().upper()
        if normalized == "SPY":
            return self.settings.spy_market_id
        if normalized == "QQQ":
            return self.settings.qqq_market_id
        raise ValueError("symbol must be SPY or QQQ")

    def _record_action(self, record: Dict[str, Any]) -> None:
        self.action_history.append(_json_value(record))
        if len(self.action_history) > 100:
            del self.action_history[:-100]
        self._persist_state_checked()

    async def _reconcile_after_write(self, record: Dict[str, Any]) -> None:
        """Boundedly confirm accepted writes against fresh account state.

        The signer response only means that the transaction was accepted by
        the API gateway.  We therefore keep its journal entry blocked until a
        subsequent account read proves the expected balance/position change.
        Transport/timeout records are deliberately excluded and remain
        unknown forever until an operator reconciles them.
        """

        pending_ids = self._pending_ids(record)
        accepted_ids = {
            str(item.get("pending_id"))
            for item in self._pending_unknown_records
            if item.get("status") == "accepted_pending_confirmation"
            and str(item.get("pending_id", "")) in pending_ids
        }
        attempts = max(1, int(self.settings.confirmation_attempts)) if accepted_ids else 1
        reconciliation: Dict[str, Any] = {
            "attempts": 0,
            "confirmed": {},
            "pending": sorted(accepted_ids),
        }
        for attempt in range(attempts):
            reconciliation["attempts"] = attempt + 1
            try:
                await self.refresh_once()
            except Exception as exc:  # pragma: no cover - network dependent
                record["reconciliation_error"] = str(exc)
                self._last_refresh_error = str(exc)
                LOGGER.exception("Post-action reconciliation failed")
            if accepted_ids:
                try:
                    confirmed = await self._reconcile_pending_records(accepted_ids)
                except Exception as exc:  # journal or parsing failure: keep block
                    record["reconciliation_error"] = str(exc)
                    LOGGER.exception("Post-action confirmation failed")
                    confirmed = {}
                if confirmed:
                    reconciliation["confirmed"].update(_json_value(confirmed))
                    accepted_ids.difference_update(confirmed)
                    reconciliation["pending"] = sorted(accepted_ids)
                if not accepted_ids:
                    break
            if attempt + 1 < attempts and self.settings.confirmation_poll_seconds > 0:
                await asyncio.sleep(self.settings.confirmation_poll_seconds)
        if accepted_ids:
            record["reconciliation_pending"] = sorted(accepted_ids)
        if reconciliation["confirmed"]:
            # Reflect each journal transition in the dashboard/action history,
            # even when another leg of a paired action is still pending.
            updated_result = record.get("result")
            for pending_id, confirmation in reconciliation["confirmed"].items():
                updated_result = self._replace_pending_result(updated_result, pending_id, confirmation)
            record["result"] = updated_result
        if accepted_ids or reconciliation["confirmed"]:
            record["reconciliation"] = reconciliation
        if self.action_history:
            self.action_history[-1] = _json_value(record)
        try:
            self._persist_state()
        except Exception:
            LOGGER.critical("Unable to persist post-action reconciliation state %s", self._state_path, exc_info=True)

    def snapshot_payload(self) -> Dict[str, Any]:
        positions_by_symbol: Dict[str, Decimal] = {"SPY": Decimal("0"), "QQQ": Decimal("0")}
        net_value_by_symbol: Dict[str, Decimal] = {"SPY": Decimal("0"), "QQQ": Decimal("0")}
        gross_value_by_symbol: Dict[str, Decimal] = {"SPY": Decimal("0"), "QQQ": Decimal("0")}
        unrealized_pnl = Decimal("0")
        realized_pnl = Decimal("0")
        leg_status: List[Dict[str, Any]] = []
        expected = {(leg.account, leg.symbol): leg for leg in self.settings.legs}
        for name, snapshot in self.snapshots.items():
            for symbol, market_id in (("SPY", self.settings.spy_market_id), ("QQQ", self.settings.qqq_market_id)):
                position = snapshot.position(symbol, market_id)
                size = position.signed_size if position else Decimal("0")
                positions_by_symbol[symbol] += size
                if position:
                    unrealized_pnl += position.unrealized_pnl
                    realized_pnl += position.realized_pnl
                    gross_value_by_symbol[symbol] += position.position_value
                    net_value_by_symbol[symbol] += (
                        position.position_value if position.signed_size > 0
                        else -position.position_value if position.signed_size < 0
                        else Decimal("0")
                    )
                expected_leg = expected.get((name, symbol))
                leg_status.append({
                    "account": name,
                    "symbol": symbol,
                    "market_id": market_id,
                    "signed_size": size,
                    "actual_side": "long" if size > 0 else "short" if size < 0 else "flat",
                    "expected_side": "long" if expected_leg and expected_leg.expected_sign > 0 else "short",
                    # A flat/missing leg is not neutral-ready.  Treating it as
                    # directionally valid made the dashboard look healthy and
                    # allowed risk transfers after a leg had disappeared.
                    "direction_ok": bool(expected_leg and size != 0 and size * expected_leg.expected_sign > 0),
                    "position_value": position.position_value if position else Decimal("0"),
                })
        first = self.snapshots.get("main")
        second = self.snapshots.get("sub")
        margin_delta = None
        available_balance_delta = None
        total_equity = None
        total_available_balance = None
        available_balance_to_total_equity_ratio = None
        if first and second and first.maintenance_ratio is not None and second.maintenance_ratio is not None:
            margin_delta = first.maintenance_ratio - second.maintenance_ratio
        if first and second:
            # Positive means main has more available collateral; negative
            # means sub has more. This is the transfer-balancing signal.
            available_balance_delta = first.available_balance - second.available_balance
            total_equity = first.equity + second.equity
            total_available_balance = first.available_balance + second.available_balance
            if total_equity > 0:
                available_balance_to_total_equity_ratio = total_available_balance / total_equity
        neutral_layout = self._neutral_layout_report() if first and second else {
            "ready": False,
            "reason": "both account snapshots are required",
            "symbols": {},
            "tolerance": self.settings.neutral_notional_tolerance,
        }
        transfer_health = self._transfer_health()
        transfer_allowed = bool(self.settings.live and transfer_health["allowed"])
        ready = len(self.snapshots) == len(self.settings.accounts)
        pending_writes = bool(self._pending_write_reason())
        healthy = (
            ready
            and not self._last_refresh_error
            and not self._pair_error
            and neutral_layout["ready"]
            and not pending_writes
            and transfer_health["state"] in {"ready", "read_only"}
        )
        transfer_history: List[Dict[str, Any]] = []
        for record in reversed(self.action_history):
            if str(record.get("type", "")).casefold() != "transfer":
                continue
            plan = record.get("plan") if isinstance(record.get("plan"), Mapping) else {}
            result = record.get("result")
            status = None
            if isinstance(result, Mapping):
                status = result.get("status")
            if not status:
                status = self._unconfirmed_status(result) or "completed"
            transfer_history.append({
                "timestamp": record.get("timestamp"),
                "source": plan.get("source"),
                "destination": plan.get("destination"),
                "amount": plan.get("amount"),
                "status": status,
                "reason": plan.get("reason"),
            })
            if len(transfer_history) >= 50:
                break
        return _json_value({
            "ok": healthy,
            "state": "healthy" if healthy else "degraded",
            "endpoint": self.settings.rest_url,
            "chain_id": self.settings.chain_id,
            "live": self.settings.live,
            "dashboard_actions_enabled": bool(self.settings.live and self.settings.dashboard_token),
            "auto_transfer": self.settings.auto_transfer,
            "pair_error": self._pair_error,
            "last_refresh_error": self._last_refresh_error,
            "transfer_block_reason": self._transfer_block_reason,
            "transfer_state": transfer_health["state"],
            "transfer_allowed": transfer_allowed,
            "transfer_health": transfer_health,
            "accounts": {name: snapshot.as_payload() for name, snapshot in self.snapshots.items()},
            "legs": leg_status,
            "neutral_layout": neutral_layout,
            "aggregate": {
                "net_spy": positions_by_symbol["SPY"],
                "net_qqq": positions_by_symbol["QQQ"],
                "net_spy_value": net_value_by_symbol["SPY"],
                "net_qqq_value": net_value_by_symbol["QQQ"],
                "gross_spy_value": gross_value_by_symbol["SPY"],
                "gross_qqq_value": gross_value_by_symbol["QQQ"],
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": realized_pnl,
                "margin_ratio_delta": margin_delta,
                "available_balance_delta": available_balance_delta,
                "total_equity": total_equity,
                "total_available_balance": total_available_balance,
                "available_balance_to_total_equity_ratio": available_balance_to_total_equity_ratio,
            },
            "transfer_plan": self.last_plan.as_payload() if self.last_plan else None,
            "account_indexes": {name: snapshot.account_index for name, snapshot in self.snapshots.items()},
            "last_transfer": self.last_transfer,
            "transfer_history": transfer_history,
            "pending_writes": self._pending_unknown_records,
            "writes_blocked": pending_writes,
            "action_history": self.action_history[-20:],
            "updated_at": time.time(),
        })

    @staticmethod
    def _feishu_value(value: Any, *, digits: int = 6) -> str:
        if value is None or value == "":
            return "-"
        try:
            number = Decimal(str(value))
            if number.is_finite():
                return f"{number:.{digits}f}".rstrip("0").rstrip(".") or "0"
        except (InvalidOperation, TypeError, ValueError):
            pass
        return str(value)

    def _feishu_report_text(self) -> str:
        """Build a compact, non-secret account summary for the Feishu bot."""

        payload = self.snapshot_payload()
        aggregate = payload.get("aggregate") if isinstance(payload.get("aggregate"), Mapping) else {}
        health = payload.get("transfer_health") if isinstance(payload.get("transfer_health"), Mapping) else {}
        lines = [
            "Lighter Robinhood 中性账户报告",
            time.strftime("时间: %Y-%m-%d %H:%M:%S %Z", time.localtime()),
            f"服务状态: {payload.get('state', '-')} | 转账状态: {payload.get('transfer_state', '-')} | "
            f"转账允许: {'是' if payload.get('transfer_allowed') else '否'}",
            f"两账户总权益: {self._feishu_value(aggregate.get('total_equity'))} USDG | "
            f"可用保证金总额: {self._feishu_value(aggregate.get('total_available_balance'))} USDG",
            f"可用保证金差值(主-子): {self._feishu_value(aggregate.get('available_balance_delta'))} USDG | "
            f"恢复进度: {health.get('recovery_successes', 0)}/{health.get('recovery_required', 0)}",
        ]
        for name, label in (("main", "主账户"), ("sub", "子账户")):
            account = payload.get("accounts", {}).get(name, {}) if isinstance(payload.get("accounts"), Mapping) else {}
            lines.append(
                f"{label}: 权益 {self._feishu_value(account.get('equity'))} | "
                f"可用 {self._feishu_value(account.get('available_balance'))} | "
                f"维持保证金 {self._feishu_value(account.get('maintenance_margin_requirement'))}"
            )
            positions = account.get("positions") if isinstance(account.get("positions"), list) else []
            position_by_symbol = {
                str(position.get("symbol", "")).upper(): position
                for position in positions
                if isinstance(position, Mapping)
            }
            for symbol in ("SPY", "QQQ"):
                position = position_by_symbol.get(symbol, {})
                lines.append(
                    f"  {symbol}: 仓位 {self._feishu_value(position.get('signed_size'))} | "
                    f"名义 {self._feishu_value(position.get('position_value'))} | "
                    f"未实现盈亏 {self._feishu_value(position.get('unrealized_pnl'))}"
                )
        if health.get("reason"):
            lines.append(f"转账限制原因: {health['reason']}")
        plan = payload.get("transfer_plan")
        if isinstance(plan, Mapping):
            lines.append(
                f"当前转账计划: {plan.get('source', '-')} -> {plan.get('destination', '-')} "
                f"{self._feishu_value(plan.get('amount'))} USDG"
            )
        return "\n".join(lines)

    async def _send_feishu_report(self) -> bool:
        """Send one report; notification errors never alter trading state."""

        webhook_url = self.settings.feishu_webhook_url.strip()
        if not webhook_url:
            return False
        if self._session is None:
            LOGGER.warning("Feishu report skipped because HTTP session is not ready")
            return False
        body: Dict[str, Any] = {
            "msg_type": "text",
            "content": {"text": self._feishu_report_text()},
        }
        if self.settings.feishu_webhook_secret:
            timestamp = str(int(time.time()))
            sign_source = f"{timestamp}\n{self.settings.feishu_webhook_secret}".encode("utf-8")
            digest = hmac.new(
                self.settings.feishu_webhook_secret.encode("utf-8"), sign_source, hashlib.sha256
            ).digest()
            body.update({"timestamp": timestamp, "sign": base64.b64encode(digest).decode("ascii")})
        timeout = aiohttp.ClientTimeout(total=min(self.settings.request_timeout_seconds, 15.0))
        try:
            async with self._session.post(webhook_url, json=body, timeout=timeout) as response:
                response_text = await response.text()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}: {response_text[:300]}")
                try:
                    response_payload = json.loads(response_text) if response_text else {}
                except json.JSONDecodeError:
                    response_payload = {}
                if isinstance(response_payload, Mapping):
                    response_code = response_payload.get("code")
                    if response_code not in (None, 0, "0", 200, "200"):
                        raise RuntimeError(
                            f"Feishu returned code {response_code}: {response_payload.get('msg', '')}"
                        )
            LOGGER.info("Feishu neutral account report sent")
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("Feishu neutral account report failed: %s", exc)
            return False

    async def _maybe_send_feishu_report(self) -> None:
        if not self.settings.feishu_webhook_url:
            return
        now = time.monotonic()
        if now < self._next_feishu_report_at:
            return
        # Reserve the next slot before awaiting network I/O. A failed webhook
        # must not be retried every five-second account polling cycle.
        self._next_feishu_report_at = now + self.settings.feishu_report_interval_seconds
        await self._send_feishu_report()

    async def run(self) -> None:
        if not self.gateways:
            await self.start()
        while not self._stop_event.is_set():
            try:
                await self.refresh_once()
                # A write may remain accepted-pending after the bounded
                # immediate confirmation window.  Each normal poll gets one
                # additional read-based confirmation opportunity; unknown
                # transport/timeout records are never touched here.
                await self._reconcile_pending_records()
                plan = await self.calculate_transfer_plan()
                transfer_allowed = (
                    not self.last_transfer
                    or time.time() - float(self.last_transfer.get("timestamp", 0))
                    >= self.settings.transfer_cooldown_seconds
                )
                if plan and self.settings.auto_transfer and self.settings.live and transfer_allowed:
                    await self.execute_transfer(plan)
                await self._maybe_send_feishu_report()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # monitor remains alive and dashboard reports stale state
                self._last_refresh_error = str(exc)
                LOGGER.exception("Neutral manager refresh failed")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.poll_seconds)
            except asyncio.TimeoutError:
                continue

    async def stop(self) -> None:
        self._stop_event.set()
        try:
            if self._dashboard is not None:
                with contextlib.suppress(Exception):
                    await self._dashboard.stop()
                self._dashboard = None
            for gateway in self.gateways.values():
                with contextlib.suppress(Exception):
                    await gateway.close()
            if self._session is not None:
                with contextlib.suppress(Exception):
                    await self._session.close()
                self._session = None
        finally:
            self._instance_lock.release()


def _env_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().casefold() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, environ: Optional[Mapping[str, Any]] = None) -> int:
    """Parse an optional integer environment value; blank means default."""

    source = os.environ if environ is None else environ
    raw = source.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def settings_from_env(env_file: Optional[str] = None) -> NeutralSettings:
    file_values: Dict[str, Any] = {}
    if env_file:
        # An explicitly supplied manager env file is the source of truth, but
        # read it without mutating the caller's process environment. This
        # avoids leaking settings into sibling services/tests.
        file_values = {
            str(key): value
            for key, value in dotenv.dotenv_values(env_file).items()
            if value is not None
        }
    effective_env: Dict[str, Any] = dict(os.environ)
    effective_env.update(file_values)

    def get(name: str, default: Any = None) -> Any:
        return effective_env.get(name, default)

    profile = resolve_lighter_endpoint_profile(
        "robinhood",
        rest_url=get("LIGHTER_BASE_URL", ROBINHOOD_MAINNET.rest_url),
        ws_url=get("LIGHTER_WS_URL", ROBINHOOD_MAINNET.ws_url),
        chain_id=get("LIGHTER_CHAIN_ID", str(ROBINHOOD_MAINNET.chain_id)),
    )
    main_index = _env_int(
        "RH_NEUTRAL_MAIN_ACCOUNT_INDEX",
        _env_int("LIGHTER_ACCOUNT_INDEX", -1, effective_env),
        effective_env,
    )
    sub_index = _env_int("RH_NEUTRAL_SUB_ACCOUNT_INDEX", -1, effective_env)
    main_keys_raw = get("RH_NEUTRAL_MAIN_API_PRIVATE_KEYS", get("LIGHTER_API_PRIVATE_KEYS", ""))
    sub_keys_raw = get("RH_NEUTRAL_SUB_API_PRIVATE_KEYS", "")
    main_keys = _parse_private_keys(main_keys_raw, label="RH_NEUTRAL_MAIN_API_PRIVATE_KEYS")
    sub_keys = _parse_private_keys(sub_keys_raw, label="RH_NEUTRAL_SUB_API_PRIVATE_KEYS")
    main_key_index = _env_int("RH_NEUTRAL_MAIN_API_KEY_INDEX", next(iter(main_keys), 4), effective_env)
    sub_key_index = _env_int("RH_NEUTRAL_SUB_API_KEY_INDEX", next(iter(sub_keys), 4), effective_env)
    settings = NeutralSettings(
        main=AccountSpec("main", main_index, main_key_index, main_keys),
        sub=AccountSpec("sub", sub_index, sub_key_index, sub_keys),
        spy_market_id=_env_int("RH_NEUTRAL_SPY_MARKET_ID", 0, effective_env),
        qqq_market_id=_env_int("RH_NEUTRAL_QQQ_MARKET_ID", 0, effective_env),
        main_long_symbol=str(get("RH_NEUTRAL_MAIN_LONG_SYMBOL", "SPY") or "SPY"),
        l1_address=str(get("RH_NEUTRAL_L1_ADDRESS", "") or "").strip(),
        poll_seconds=float(get("RH_NEUTRAL_POLL_SECONDS", "5")),
        transfer_snapshot_max_age_seconds=float(get("RH_NEUTRAL_TRANSFER_SNAPSHOT_MAX_AGE_SECONDS", "15")),
        transfer_recovery_successes_required=_env_int(
            "RH_NEUTRAL_TRANSFER_RECOVERY_SUCCESSES", 3, effective_env
        ),
        min_margin_ratio=_decimal(get("RH_NEUTRAL_MIN_MARGIN_RATIO", "1.5")),
        target_margin_ratio=_decimal(get("RH_NEUTRAL_TARGET_MARGIN_RATIO", "2.0")),
        reserve_usdc=_decimal(get("RH_NEUTRAL_RESERVE_USDC", "50")),
        transfer_hysteresis_usdc=_decimal(get("RH_NEUTRAL_TRANSFER_HYSTERESIS_USDC", "10")),
        max_transfer_usdc=_decimal(get("RH_NEUTRAL_MAX_TRANSFER_USDC", "1000")),
        min_transfer_usdc=_decimal(get("RH_NEUTRAL_MIN_TRANSFER_USDC", "1")),
        transfer_cooldown_seconds=float(get("RH_NEUTRAL_TRANSFER_COOLDOWN_SECONDS", "30")),
        close_slippage_bps=_decimal(get("RH_NEUTRAL_CLOSE_SLIPPAGE_BPS", "50")),
        neutral_notional_tolerance=_decimal(get("RH_NEUTRAL_NOTIONAL_TOLERANCE", "0.50")),
        live=_env_bool(get("RH_NEUTRAL_LIVE"), False),
        auto_transfer=_env_bool(get("RH_NEUTRAL_AUTO_TRANSFER"), False),
        dashboard_host=str(get("RH_NEUTRAL_DASHBOARD_HOST", "127.0.0.1")),
        dashboard_port=_env_int("RH_NEUTRAL_DASHBOARD_PORT", 8790, effective_env),
        dashboard_token=str(get("RH_NEUTRAL_DASHBOARD_TOKEN", "") or ""),
        dashboard_username=str(get("RH_NEUTRAL_DASHBOARD_USERNAME", "operator")),
        dashboard_allow_public_bind=_env_bool(get("RH_NEUTRAL_DASHBOARD_ALLOW_PUBLIC"), False),
        request_timeout_seconds=float(get("RH_NEUTRAL_REQUEST_TIMEOUT_SECONDS", "10")),
        action_timeout_seconds=float(get("RH_NEUTRAL_ACTION_TIMEOUT_SECONDS", "20")),
        confirmation_attempts=_env_int("RH_NEUTRAL_CONFIRMATION_ATTEMPTS", 3, effective_env),
        confirmation_poll_seconds=float(get("RH_NEUTRAL_CONFIRMATION_POLL_SECONDS", "0.5")),
        feishu_webhook_url=str(get("RH_NEUTRAL_FEISHU_WEBHOOK_URL", "") or "").strip(),
        feishu_webhook_secret=str(get("RH_NEUTRAL_FEISHU_WEBHOOK_SECRET", "") or "").strip(),
        feishu_report_interval_seconds=float(get("RH_NEUTRAL_FEISHU_REPORT_INTERVAL_SECONDS", "600")),
        state_path=str(get("RH_NEUTRAL_STATE_PATH", "logs/rh_neutral_manager_state.json")),
        rest_url=profile.rest_url,
        ws_url=profile.ws_url,
        chain_id=profile.chain_id,
    )
    settings.validate(require_market_ids=False)
    return settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor and balance two RH Lighter accounts")
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--live", action="store_true", help="Enable real transfers and reduce-only close orders")
    parser.add_argument("--auto-transfer", action="store_true", help="Automatically execute safe transfer plans")
    parser.add_argument("--main-account-index", type=int)
    parser.add_argument("--sub-account-index", type=int)
    parser.add_argument("--l1-address")
    parser.add_argument("--spy-market-id", type=int)
    parser.add_argument("--qqq-market-id", type=int)
    parser.add_argument(
        "--main-long-symbol",
        choices=("SPY", "QQQ"),
        type=str.upper,
        help="Symbol that is long on main; subaccount is automatically opposite (default: SPY)",
    )
    parser.add_argument("--host", dest="dashboard_host")
    parser.add_argument("--port", dest="dashboard_port", type=int)
    parser.add_argument("--dashboard-token")
    parser.add_argument("--dashboard-username")
    parser.add_argument("--poll-seconds", type=float)
    parser.add_argument("--transfer-snapshot-max-age-seconds", type=float)
    parser.add_argument("--transfer-recovery-successes", type=int)
    parser.add_argument("--notional-tolerance", dest="neutral_notional_tolerance", type=Decimal)
    parser.add_argument("--confirmation-attempts", type=int)
    parser.add_argument("--confirmation-poll-seconds", type=float)
    parser.add_argument("--feishu-webhook-url")
    parser.add_argument("--feishu-webhook-secret")
    parser.add_argument("--feishu-report-interval-seconds", type=float)
    return parser


async def _run(settings: NeutralSettings) -> None:
    manager = NeutralPositionManager(settings)
    await manager.start()
    loop = asyncio.get_running_loop()
    installed_signals = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, manager._stop_event.set)
            installed_signals.append(signum)
        except (NotImplementedError, RuntimeError):
            # Windows and embedded event loops may not support signal handlers;
            # KeyboardInterrupt still reaches the outer ``main`` handler.
            continue
    try:
        await manager.run()
    finally:
        for signum in installed_signals:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(signum)
        await manager.stop()


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    settings = settings_from_env(args.env_file)
    if args.live:
        settings.live = True
    if args.auto_transfer:
        settings.auto_transfer = True
    if args.main_account_index is not None:
        settings.main = AccountSpec(
            settings.main.name,
            args.main_account_index,
            settings.main.api_key_index,
            settings.main.api_private_keys,
        )
    if args.sub_account_index is not None:
        settings.sub = AccountSpec(
            settings.sub.name,
            args.sub_account_index,
            settings.sub.api_key_index,
            settings.sub.api_private_keys,
        )
    if args.l1_address is not None:
        settings.l1_address = args.l1_address.strip()
    if args.spy_market_id is not None:
        settings.spy_market_id = args.spy_market_id
    if args.qqq_market_id is not None:
        settings.qqq_market_id = args.qqq_market_id
    if args.main_long_symbol is not None:
        settings.main_long_symbol = args.main_long_symbol
    if args.dashboard_host is not None:
        settings.dashboard_host = args.dashboard_host
    if args.dashboard_port is not None:
        settings.dashboard_port = args.dashboard_port
    if args.dashboard_token is not None:
        settings.dashboard_token = args.dashboard_token
    if args.dashboard_username is not None:
        settings.dashboard_username = args.dashboard_username
    if args.poll_seconds is not None:
        settings.poll_seconds = args.poll_seconds
    if args.transfer_snapshot_max_age_seconds is not None:
        settings.transfer_snapshot_max_age_seconds = args.transfer_snapshot_max_age_seconds
    if args.transfer_recovery_successes is not None:
        settings.transfer_recovery_successes_required = args.transfer_recovery_successes
    if args.neutral_notional_tolerance is not None:
        settings.neutral_notional_tolerance = args.neutral_notional_tolerance
    if args.confirmation_attempts is not None:
        settings.confirmation_attempts = args.confirmation_attempts
    if args.confirmation_poll_seconds is not None:
        settings.confirmation_poll_seconds = args.confirmation_poll_seconds
    if args.feishu_webhook_url is not None:
        settings.feishu_webhook_url = args.feishu_webhook_url.strip()
    if args.feishu_webhook_secret is not None:
        settings.feishu_webhook_secret = args.feishu_webhook_secret.strip()
    if args.feishu_report_interval_seconds is not None:
        settings.feishu_report_interval_seconds = args.feishu_report_interval_seconds
    settings.validate(require_market_ids=False)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(_run(settings))
    except KeyboardInterrupt:
        LOGGER.info("Neutral manager stopped")


if __name__ == "__main__":
    main()
