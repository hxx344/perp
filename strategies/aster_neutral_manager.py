"""Aster two-account XAUUSD1 neutral monitor and balance manager.

This module is deliberately separate from the Robinhood Lighter neutral
manager. It uses Aster's signed Futures API for account reads and only enables
master/sub-account transfers when an explicit wallet EIP-712 signer is
configured. Read-only monitoring works with one HMAC API key pair per account.

The supported layout is intentionally small and explicit:
  main account: XAUUSD1 long
  sub account:  XAUUSD1 short

Account balancing uses the two fresh ``availableBalance`` values, while a
transfer is capped by each source account's Aster ``maxWithdrawAmount``.
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
import signal
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlencode, urlparse

import aiohttp
import dotenv

from strategies.neutral_dashboard import NeutralDashboard

LOGGER = logging.getLogger("strategies.aster_neutral_manager")
ASTER_REST_URL = "https://fapi.asterdex.com"
ASTER_TRANSFER_REST_URL = "https://fapi.asterdex.com"
TRANSFER_KIND = "FUTURE_FUTURE"
TRANSFER_ASSET = "USD1"


class _AsterInstanceLock:
    """Process lock for one Aster neutral manager state path."""

    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def acquire(self) -> None:
        if self.handle is not None:
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
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            handle.close()
            raise RuntimeError(f"another Aster neutral manager owns {self.path.resolve()}") from exc
        self.handle = handle
        try:
            handle.seek(0)
            handle.truncate()
            handle.write((json.dumps({"pid": os.getpid(), "started_at": int(time.time())}) + "\n").encode("ascii"))
            handle.flush()
            os.fsync(handle.fileno())
        except OSError:
            pass

    def release(self) -> None:
        handle = self.handle
        self.handle = None
        if handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _decimal(value: Any, default: Optional[Decimal] = Decimal("0")) -> Optional[Decimal]:
    if value is None:
        return default
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


def _required_decimal(value: Any, label: str) -> Decimal:
    parsed = _decimal(value, None)
    if parsed is None:
        raise ValueError(f"{label} must be a finite decimal")
    return parsed


def _first(mapping: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def _env_bool(value: Any, default: bool = False) -> bool:
    if value is None or not str(value).strip():
        return default
    return str(value).strip().casefold() in {"1", "true", "yes", "on"}


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return _env_bool(value, default)


def _env_int(name: str, default: int, env: Mapping[str, Any]) -> int:
    value = env.get(name)
    if value is None or not str(value).strip():
        return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _format_amount(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN), "f").rstrip("0").rstrip(".") or "0"


@dataclass(frozen=True, slots=True)
class AsterAccountSpec:
    name: str
    api_key: str = ""
    api_secret: str = ""
    wallet_address: str = ""
    user_address: str = ""
    signer_address: str = ""
    signer_private_key: str = ""

    @property
    def effective_user_address(self) -> str:
        return (self.user_address or self.wallet_address).strip()

    @property
    def uses_pro_api(self) -> bool:
        return bool(self.user_address or self.signer_address or self.signer_private_key)


@dataclass(slots=True)
class AsterPositionSnapshot:
    symbol: str
    signed_size: Decimal
    position_value: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    liquidation_price: Decimal
    leverage: Decimal
    isolated: bool

    def as_payload(self) -> Dict[str, Any]:
        return _json_value({
            "symbol": self.symbol,
            "signed_size": self.signed_size,
            "position_value": self.position_value,
            "entry_price": self.entry_price,
            "mark_price": self.mark_price,
            "unrealized_pnl": self.unrealized_pnl,
            "liquidation_price": self.liquidation_price,
            "leverage": self.leverage,
            "isolated": self.isolated,
        })


@dataclass(slots=True)
class AsterAccountSnapshot:
    name: str
    account_alias: str
    available_balance: Decimal
    max_withdraw_amount: Decimal
    wallet_balance: Decimal
    equity: Decimal
    unrealized_pnl: Decimal
    initial_margin: Decimal
    maintenance_margin: Decimal
    can_trade: bool
    can_withdraw: bool
    position: Optional[AsterPositionSnapshot]
    observed_at: float
    error: Optional[str] = None

    @property
    def maintenance_usage_ratio(self) -> Optional[Decimal]:
        if self.equity <= 0:
            return None
        return self.maintenance_margin / self.equity

    @property
    def initial_usage_ratio(self) -> Optional[Decimal]:
        if self.equity <= 0:
            return None
        return self.initial_margin / self.equity

    def as_payload(self) -> Dict[str, Any]:
        return _json_value({
            "name": self.name,
            "account_alias": self.account_alias,
            "available_balance": self.available_balance,
            "max_withdraw_amount": self.max_withdraw_amount,
            "wallet_balance": self.wallet_balance,
            "equity": self.equity,
            "unrealized_pnl": self.unrealized_pnl,
            "initial_margin": self.initial_margin,
            "maintenance_margin": self.maintenance_margin,
            "maintenance_usage_ratio": self.maintenance_usage_ratio,
            "initial_usage_ratio": self.initial_usage_ratio,
            "can_trade": self.can_trade,
            "can_withdraw": self.can_withdraw,
            "position": self.position.as_payload() if self.position else None,
            "observed_at": self.observed_at,
            "error": self.error,
        })


@dataclass(frozen=True, slots=True)
class AsterTransferPlan:
    source: str
    destination: str
    amount: Decimal
    reason: str

    def as_payload(self) -> Dict[str, Any]:
        return _json_value({
            "source": self.source,
            "destination": self.destination,
            "amount": self.amount,
            "reason": self.reason,
        })


@dataclass(slots=True)
class AsterNeutralSettings:
    main: AsterAccountSpec
    sub: AsterAccountSpec
    symbol: str = "XAUUSD1"
    asset: str = TRANSFER_ASSET
    rest_url: str = ASTER_REST_URL
    poll_seconds: float = 5.0
    snapshot_max_age_seconds: float = 15.0
    recovery_successes_required: int = 3
    transfer_hysteresis: Decimal = Decimal("10")
    min_transfer: Decimal = Decimal("1")
    max_transfer: Decimal = Decimal("1000")
    transfer_cooldown_seconds: float = 30.0
    live: bool = False
    auto_transfer: bool = False
    transfers_enabled: bool = False
    wallet_private_key: str = ""
    wallet_address: str = ""
    transfer_signer_address: str = ""
    transfer_signer_private_key: str = ""
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8791
    dashboard_username: str = "operator"
    dashboard_token: str = ""
    dashboard_allow_public_bind: bool = False
    feishu_webhook_url: str = ""
    feishu_webhook_secret: str = ""
    feishu_interval_seconds: float = 600.0
    alert_threshold: Decimal = Decimal("100")
    request_timeout_seconds: float = 10.0
    action_timeout_seconds: float = 20.0
    state_path: str = "logs/aster_neutral_manager_state.json"

    @property
    def effective_master_wallet_address(self) -> str:
        return (self.wallet_address or self.main.effective_user_address).strip()

    @property
    def effective_sub_wallet_address(self) -> str:
        return (self.sub.wallet_address or self.sub.effective_user_address).strip()

    @property
    def effective_transfer_signer_address(self) -> str:
        return (self.transfer_signer_address or self.main.signer_address).strip()

    @property
    def effective_transfer_signer_private_key(self) -> str:
        return (self.transfer_signer_private_key or self.main.signer_private_key or self.wallet_private_key).strip()

    def validate(self) -> None:
        self.symbol = self.symbol.strip().upper()
        self.asset = self.asset.strip().upper()
        if not self.symbol:
            raise ValueError("Aster neutral symbol must not be empty")
        if self.asset != TRANSFER_ASSET:
            raise ValueError("Aster neutral transfer asset must be USD1 for XAUUSD1")
        if self.main.name != "main" or self.sub.name != "sub":
            raise ValueError("Aster neutral account names must be main and sub")
        for spec in (self.main, self.sub):
            if spec.uses_pro_api:
                if not spec.effective_user_address or not spec.signer_address or not spec.signer_private_key:
                    raise ValueError(f"{spec.name} Pro API requires user_address, signer_address, and signer_private_key")
                for label, address in ((f"{spec.name} user address", spec.effective_user_address), (f"{spec.name} signer address", spec.signer_address)):
                    if len(address) != 42 or not address.lower().startswith("0x") or any(char not in "0123456789abcdefABCDEF" for char in address[2:]):
                        raise ValueError(f"{label} must be a 20-byte 0x-prefixed EVM address")
                key = spec.signer_private_key[2:] if spec.signer_private_key.lower().startswith("0x") else spec.signer_private_key
                if len(key) != 64 or any(char not in "0123456789abcdefABCDEF" for char in key):
                    raise ValueError(f"{spec.name} signer_private_key must contain 64 hexadecimal characters")
            elif not spec.api_key or not spec.api_secret:
                raise ValueError(f"{spec.name} requires either Pro API signer credentials or API key/secret")
        if self.main.api_key and self.sub.api_key and self.main.api_key == self.sub.api_key:
            raise ValueError("Aster main and sub API keys must be different")
        for name, value in (("poll_seconds", self.poll_seconds), ("snapshot_max_age_seconds", self.snapshot_max_age_seconds),
                            ("transfer_cooldown_seconds", self.transfer_cooldown_seconds),
                            ("feishu_interval_seconds", self.feishu_interval_seconds),
                            ("request_timeout_seconds", self.request_timeout_seconds),
                            ("action_timeout_seconds", self.action_timeout_seconds)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.recovery_successes_required <= 0:
            raise ValueError("recovery_successes_required must be positive")
        if self.min_transfer <= 0 or self.max_transfer < self.min_transfer:
            raise ValueError("transfer amount limits are invalid")
        if self.transfer_hysteresis < 0:
            raise ValueError("transfer_hysteresis must not be negative")
        if not self.alert_threshold.is_finite() or self.alert_threshold <= 0:
            raise ValueError("alert_threshold must be positive")
        if self.dashboard_host not in {"127.0.0.1", "::1"}:
            if not self.dashboard_allow_public_bind or len(self.dashboard_token) < 16:
                raise ValueError("public Aster dashboard requires allow_public_bind and a 16+ character token")
        if self.dashboard_token and len(self.dashboard_token) < 16:
            raise ValueError("dashboard token must contain at least 16 characters")
        if self.feishu_webhook_url:
            parsed = urlparse(self.feishu_webhook_url)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError("Feishu webhook URL must use HTTPS")
        if self.live and not self.effective_master_wallet_address:
            raise ValueError("live Aster master/sub transfers require the master wallet address")
        if self.live:
            for label, address in (("master wallet address", self.effective_master_wallet_address), ("sub wallet address", self.effective_sub_wallet_address)):
                normalized_address = str(address).strip()
                if len(normalized_address) != 42 or not normalized_address.lower().startswith("0x") or any(
                    char not in "0123456789abcdefABCDEF" for char in normalized_address[2:]
                ):
                    raise ValueError(f"{label} must be a 20-byte 0x-prefixed EVM address")
            signer_key = self.effective_transfer_signer_private_key
            private_key = str(signer_key).strip()
            if self.transfer_signer_private_key and not self.transfer_signer_address:
                raise ValueError("transfer_signer_address is required with transfer_signer_private_key")
            if self.transfer_signer_address:
                normalized_signer = str(self.transfer_signer_address).strip()
                if len(normalized_signer) != 42 or not normalized_signer.lower().startswith("0x") or any(
                    char not in "0123456789abcdefABCDEF" for char in normalized_signer[2:]
                ):
                    raise ValueError("transfer_signer_address must be a 20-byte 0x-prefixed EVM address")
            if private_key.lower().startswith("0x"):
                private_key = private_key[2:]
            if len(private_key) != 64 or any(char not in "0123456789abcdefABCDEF" for char in private_key):
                raise ValueError("wallet_private_key must contain 64 hexadecimal characters")


def build_transfer_plan(main: AsterAccountSnapshot, sub: AsterAccountSnapshot, settings: AsterNeutralSettings) -> Optional[AsterTransferPlan]:
    if main.error or sub.error or not main.can_withdraw or not sub.can_withdraw:
        return None
    if main.position is None or sub.position is None:
        return None
    if main.position.signed_size <= 0 or sub.position.signed_size >= 0:
        return None
    delta = main.available_balance - sub.available_balance
    if abs(delta) <= settings.transfer_hysteresis:
        return None
    source, destination = (main, sub) if delta > 0 else (sub, main)
    amount = min(abs(delta) / Decimal("2"), settings.max_transfer, source.max_withdraw_amount)
    amount = amount.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    if amount < settings.min_transfer:
        return None
    return AsterTransferPlan(
        source=source.name,
        destination=destination.name,
        amount=amount,
        reason=f"available balance imbalance: main={main.available_balance}, sub={sub.available_balance}",
    )


class AsterAccountClient:
    def __init__(self, spec: AsterAccountSpec, settings: AsterNeutralSettings, session: aiohttp.ClientSession):
        self.spec = spec
        self.settings = settings
        self.session = session
        self._last_nonce = 0

    def _signed_params(self, params: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        result = dict(params or {})
        now = int(time.time() * 1000)
        result.setdefault("timestamp", now)
        result.setdefault("recvWindow", 5000)
        query = urlencode(result)
        result["signature"] = hmac.new(self.spec.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return result

    def _pro_signed_params(self, params: Optional[Mapping[str, Any]] = None) -> List[Tuple[str, str]]:
        """Build the exact V3 query string and its EIP-712 signature."""

        try:
            from eth_account import Account
            from eth_account.messages import encode_typed_data
        except ImportError as exc:
            raise RuntimeError("eth-account is required for Aster Pro API V3") from exc
        values: List[Tuple[str, str]] = [(str(key), str(value)) for key, value in (params or {}).items()]
        nonce = max(int(time.time() * 1_000_000), self._last_nonce + 1)
        self._last_nonce = nonce
        values.extend([
            ("nonce", str(nonce)),
            ("user", self.spec.effective_user_address),
            ("signer", self.spec.signer_address),
        ])
        message_text = urlencode(values)
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [{"name": "msg", "type": "string"}],
            },
            "primaryType": "Message",
            "domain": {
                "name": "AsterSignTransaction",
                "version": "1",
                "chainId": 1666,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {"msg": message_text},
        }
        private_key = self.spec.signer_private_key
        if not private_key.lower().startswith("0x"):
            private_key = "0x" + private_key
        signature = Account.sign_message(encode_typed_data(full_message=typed_data), private_key).signature.hex()
        values.append(("signature", signature))
        return values

    async def request(self, method: str, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        if self.spec.uses_pro_api:
            request_params: Any = self._pro_signed_params(params)
            headers = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "perp-aster-neutral/1.0"}
        else:
            request_params = self._signed_params(params)
            headers = {"X-MBX-APIKEY": self.spec.api_key, "Content-Type": "application/x-www-form-urlencoded"}
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout_seconds)
        url = f"{self.settings.rest_url.rstrip('/')}{path}"
        async with self.session.request(method.upper(), url, params=request_params if method.upper() == "GET" else None,
                                        data=request_params if method.upper() != "GET" else None,
                                        headers=headers, timeout=timeout) as response:
            text = await response.text()
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Aster {path} returned invalid JSON: {text[:200]}") from exc
            if response.status != 200:
                raise RuntimeError(f"Aster {path} returned HTTP {response.status}: {text[:300]}")
            if isinstance(payload, Mapping) and payload.get("code") not in (None, 0, 200, "0", "200"):
                raise RuntimeError(f"Aster {path} returned code {payload.get('code')}: {payload.get('msg', '')}")
            return payload

    async def fetch_snapshot(self) -> AsterAccountSnapshot:
        payload = await self.request("GET", "/fapi/v3/accountWithJoinMargin" if self.spec.uses_pro_api else "/fapi/v4/account")
        if not isinstance(payload, Mapping):
            raise RuntimeError("Aster account response must be an object")
        asset_rows = payload.get("assets") if isinstance(payload.get("assets"), list) else []
        asset_row = next(
            (item for item in asset_rows if isinstance(item, Mapping) and str(item.get("asset", "")).upper() == self.settings.asset),
            None,
        )
        source = asset_row or payload
        available = _required_decimal(_first(source, "availableBalance"), "availableBalance")
        max_withdraw = _required_decimal(_first(source, "maxWithdrawAmount"), "maxWithdrawAmount")
        wallet = _required_decimal(_first(source, "walletBalance", "totalWalletBalance", "totalCrossWalletBalance"), "wallet balance")
        unrealized = _required_decimal(_first(source, "unrealizedProfit", "totalUnrealizedProfit", "totalCrossUnPnl"), "unrealized profit")
        equity = _required_decimal(_first(source, "marginBalance", "totalMarginBalance", "totalCrossMarginBalance"), "margin balance")
        initial = _required_decimal(_first(source, "initialMargin", "totalInitialMargin"), "initial margin")
        maintenance = _required_decimal(_first(source, "maintMargin", "totalMaintMargin"), "maintenance margin")
        if self.settings.live and asset_row is None:
            raise RuntimeError(f"Aster account did not return a {self.settings.asset} asset row")
        if min(available, max_withdraw, wallet, initial, maintenance) < 0:
            raise RuntimeError("Aster account returned a negative transferable balance or margin field")
        can_trade_raw = _first(payload, "canTrade")
        can_withdraw_raw = _first(payload, "canWithdraw")
        if self.settings.live and (can_trade_raw is None or can_withdraw_raw is None):
            raise RuntimeError("Aster account response omitted canTrade/canWithdraw safety fields")
        positions = payload.get("positions") if isinstance(payload.get("positions"), list) else []
        matching = [item for item in positions if isinstance(item, Mapping) and str(item.get("symbol", "")).upper() == self.settings.symbol]
        # Some account wrappers omit mark/notional/liquidation fields. Query
        # the authoritative position-risk endpoint only in that case; this
        # avoids a second request during normal polling while fixing zeroed
        # display fields for affected accounts.
        if matching and any(
            _required_decimal(_first(item, "markPrice", "mark_price", "mark", default=0), "markPrice") <= 0
            or (_decimal(_first(item, "notional", "positionValue", "position_value"), None) or Decimal("0")) <= 0
            or _required_decimal(_first(item, "liquidationPrice", "liquidation_price", "liqPrice", default=0), "liquidation price") <= 0
            for item in matching
        ):
            try:
                risk_payload = await self.request("GET", "/fapi/v3/positionRisk", {"symbol": self.settings.symbol})
                risk_rows = risk_payload if isinstance(risk_payload, list) else risk_payload.get("positions", []) if isinstance(risk_payload, Mapping) else []
                risk_matching = [item for item in risk_rows if isinstance(item, Mapping) and str(item.get("symbol", "")).upper() == self.settings.symbol]
                if risk_matching:
                    matching = risk_matching
            except Exception as exc:
                LOGGER.warning("Aster positionRisk fallback failed for %s: %s", self.spec.name, exc)
        if len(matching) > 1:
            # Hedge mode can expose LONG and SHORT rows; aggregate them rather
            # than silently selecting one side.
            raw_size = sum((_required_decimal(_first(item, "positionAmt", "position_amt", "position", default=0), "positionAmt") for item in matching), Decimal("0"))
            raw_values = matching
        else:
            raw_size = _required_decimal(_first(matching[0], "positionAmt", "position_amt", "position", default=0), "positionAmt") if matching else Decimal("0")
            raw_values = matching
        position = None
        if raw_values:
            item = next((candidate for candidate in raw_values if _required_decimal(_first(candidate, "markPrice", "mark_price", "mark", default=0), "markPrice") > 0), raw_values[0])
            mark = _required_decimal(_first(item, "markPrice", "mark_price", "mark", default=0), "markPrice")
            entry = _required_decimal(_first(item, "entryPrice", "entry_price", "avgPrice", default=0), "entryPrice")
            direct_notionals = [
                abs(value) for value in (_decimal(_first(candidate, "notional", "positionValue", "position_value"), None) for candidate in raw_values)
                if value is not None and value > 0
            ]
            direct_notional = sum(direct_notionals, Decimal("0")) if direct_notionals else None
            notional = abs(direct_notional) if direct_notional is not None and direct_notional > 0 else abs(raw_size * mark)
            position = AsterPositionSnapshot(
                symbol=self.settings.symbol,
                signed_size=raw_size,
                position_value=notional,
                entry_price=entry,
                mark_price=mark,
                unrealized_pnl=sum((_required_decimal(_first(candidate, "unRealizedProfit", "unrealizedProfit", "unrealized_pnl", default=0), "position PnL") for candidate in raw_values), Decimal("0")),
                liquidation_price=next((value for value in (
                    _required_decimal(_first(candidate, "liquidationPrice", "liquidation_price", "liqPrice", default=0), "liquidation price")
                    for candidate in raw_values
                ) if value > 0), Decimal("0")),
                leverage=_required_decimal(_first(item, "leverage", "initialLeverage", default=0), "leverage"),
                isolated=str(_first(item, "marginType", "margin_type", default="")).casefold() == "isolated" or _as_bool(_first(item, "isolated", default=False), False),
            )
            # For non-USDT settlement assets, the account-level totals may be
            # omitted or zero in older API wrappers. Prefer target-position
            # requirements when they are present.
            position_initial = sum((_decimal(_first(item, "initialMargin", "positionInitialMargin", "initial_margin"), Decimal("0")) or Decimal("0") for item in raw_values), Decimal("0"))
            position_maintenance = sum((_decimal(_first(item, "maintMargin", "maintenanceMargin", "maintenance_margin"), Decimal("0")) or Decimal("0") for item in raw_values), Decimal("0"))
            if position_initial > 0:
                initial = position_initial
            if position_maintenance > 0:
                maintenance = position_maintenance
        return AsterAccountSnapshot(
            name=self.spec.name,
            account_alias=str(_first(payload, "accountAlias", "accountAliasCode", default="")),
            available_balance=available,
            max_withdraw_amount=max_withdraw,
            wallet_balance=wallet,
            equity=equity,
            unrealized_pnl=unrealized,
            initial_margin=initial,
            maintenance_margin=maintenance,
            can_trade=_as_bool(can_trade_raw, False),
            can_withdraw=_as_bool(can_withdraw_raw, False),
            position=position,
            observed_at=time.time(),
        )

    async def close(self) -> None:
        return None


class AsterNeutralManager:
    def __init__(self, settings: AsterNeutralSettings):
        settings.validate()
        self.settings = settings
        self._state_path = Path(settings.state_path).expanduser()
        if not self._state_path.is_absolute():
            self._state_path = Path.cwd() / self._state_path
        self._instance_lock = _AsterInstanceLock(self._state_path.with_suffix(self._state_path.suffix + ".lock"))
        self.snapshots: Dict[str, AsterAccountSnapshot] = {}
        self.last_plan: Optional[AsterTransferPlan] = None
        self.last_transfer: Optional[Dict[str, Any]] = None
        self.transfer_history: List[Dict[str, Any]] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._clients: Dict[str, AsterAccountClient] = {}
        self._dashboard: Optional[NeutralDashboard] = None
        self._stop_event = asyncio.Event()
        self._action_lock = asyncio.Lock()
        self._last_refresh_error: Optional[str] = None
        self._recovery_successes = 0
        self._pending_transfer: Optional[Dict[str, Any]] = None
        self._last_transfer_at = 0.0
        self._next_feishu_report = 0.0
        self._load_state()

    def _load_state(self) -> None:
        try:
            if not self._state_path.exists():
                return
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping):
                raise ValueError("state root must be an object")
            last_transfer = payload.get("last_transfer")
            pending_transfer = payload.get("pending_transfer")
            if isinstance(last_transfer, Mapping):
                self.last_transfer = dict(last_transfer)
                self._last_transfer_at = float(last_transfer.get("timestamp", 0) or 0)
            history = payload.get("transfer_history")
            if isinstance(history, list):
                self.transfer_history = [dict(item) for item in history[-50:] if isinstance(item, Mapping)]
            if isinstance(pending_transfer, Mapping):
                self._pending_transfer = dict(pending_transfer)
        except Exception as exc:
            LOGGER.error("Unable to load Aster neutral state %s: %s", self._state_path, exc)
            self._pending_transfer = {"status": "unknown_journal", "error": str(exc)}

    def _persist_state(self) -> None:
        payload = _json_value({
            "version": 1,
            "updated_at": time.time(),
            "last_transfer": self.last_transfer,
            "transfer_history": self.transfer_history[-50:],
            "pending_transfer": self._pending_transfer,
        })
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self._state_path.with_suffix(self._state_path.suffix + f".tmp.{os.getpid()}")
        temp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        with contextlib.suppress(OSError):
            os.chmod(temp, 0o600)
        os.replace(temp, self._state_path)

    async def start(self) -> None:
        if self._session is not None:
            return
        self._instance_lock.acquire()
        try:
            self._session = aiohttp.ClientSession()
            self._clients = {spec.name: AsterAccountClient(spec, self.settings, self._session) for spec in (self.settings.main, self.settings.sub)}
            await self.refresh_once()
            self._dashboard = NeutralDashboard(
                self.snapshot_payload,
                self._handle_dashboard_action,
                host=self.settings.dashboard_host,
                port=self.settings.dashboard_port,
                username=self.settings.dashboard_username if self.settings.dashboard_token else None,
                password=self.settings.dashboard_token or None,
                allow_public_bind=self.settings.dashboard_allow_public_bind,
                allowed_accounts=("main", "sub"),
                allowed_symbols=(self.settings.symbol,),
            )
            self._dashboard._page_path = Path(__file__).with_name("aster_neutral_dashboard.html")
            await self._dashboard.start()
        except Exception:
            await self.stop()
            raise

    async def refresh_once(self) -> Dict[str, Any]:
        if not self._clients:
            if self._session is None:
                self._session = aiohttp.ClientSession()
            self._clients = {spec.name: AsterAccountClient(spec, self.settings, self._session) for spec in (self.settings.main, self.settings.sub)}
        results = await asyncio.gather(*(client.fetch_snapshot() for client in self._clients.values()), return_exceptions=True)
        self._last_refresh_error = None
        healthy = True
        for name, result in zip(self._clients, results):
            if isinstance(result, BaseException):
                healthy = False
                old = self.snapshots.get(name)
                if old:
                    old.error = str(result)
                    self.snapshots[name] = old
                else:
                    self.snapshots[name] = AsterAccountSnapshot(name, "", Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), False, False, None, time.time(), str(result))
                self._last_refresh_error = str(result)
            else:
                self.snapshots[name] = result
        if healthy and len(self.snapshots) == 2 and all(not item.error for item in self.snapshots.values()):
            self._recovery_successes = min(self._recovery_successes + 1, self.settings.recovery_successes_required)
        else:
            self._recovery_successes = 0
        return self.snapshot_payload()

    def _health(self) -> Dict[str, Any]:
        ages = {
            name: max(0.0, time.time() - snapshot.observed_at) if snapshot else None
            for name, snapshot in (("main", self.snapshots.get("main")), ("sub", self.snapshots.get("sub")))
        }
        if self._pending_transfer:
            return {"state": "blocked", "allowed": False, "reason": "previous Aster transfer status is unknown; reconcile it first", "snapshot_ages": ages, "recovery_successes": self._recovery_successes, "recovery_required": self.settings.recovery_successes_required}
        if self._last_refresh_error or len(self.snapshots) != 2:
            return {"state": "blocked", "allowed": False, "reason": self._last_refresh_error or "both Aster account snapshots are required", "snapshot_ages": ages, "recovery_successes": self._recovery_successes, "recovery_required": self.settings.recovery_successes_required}
        failed = next((item for item in self.snapshots.values() if item.error), None)
        if failed:
            return {"state": "blocked", "allowed": False, "reason": f"{failed.name} account read failed: {failed.error}", "snapshot_ages": ages, "recovery_successes": 0, "recovery_required": self.settings.recovery_successes_required}
        if self.settings.live and any(age is None or age > self.settings.snapshot_max_age_seconds for age in ages.values()):
            return {"state": "blocked", "allowed": False, "reason": "Aster account snapshot is stale", "snapshot_ages": ages, "recovery_successes": self._recovery_successes, "recovery_required": self.settings.recovery_successes_required}
        if self.settings.live and self._recovery_successes < self.settings.recovery_successes_required:
            return {"state": "recovering", "allowed": False, "reason": "waiting for consecutive healthy Aster account reads", "snapshot_ages": ages, "recovery_successes": self._recovery_successes, "recovery_required": self.settings.recovery_successes_required}
        if not self.settings.live:
            return {"state": "read_only", "allowed": False, "reason": "live mode is disabled; Aster transfers are read-only", "snapshot_ages": ages, "recovery_successes": self._recovery_successes, "recovery_required": self.settings.recovery_successes_required}
        return {"state": "ready", "allowed": True, "reason": None, "snapshot_ages": ages, "recovery_successes": self._recovery_successes, "recovery_required": self.settings.recovery_successes_required}

    def _transfer_allowed(self) -> None:
        if not self.settings.transfers_enabled:
            raise RuntimeError("Aster transfers are disabled; set ASTER_NEUTRAL_ENABLE_TRANSFERS=true")
        health = self._health()
        if health["state"] != "ready":
            raise RuntimeError(str(health["reason"]))
        if not self.settings.effective_master_wallet_address:
            raise RuntimeError("Aster master wallet address is not configured")
        if not self.settings.effective_transfer_signer_private_key:
            raise RuntimeError("Aster transfer signer is not configured")
        remaining = self.settings.transfer_cooldown_seconds - (time.time() - self._last_transfer_at)
        if self._last_transfer_at and remaining > 0:
            raise RuntimeError(f"Aster transfer cooldown is active for another {remaining:.1f}s")

    async def calculate_transfer_plan(self) -> Optional[AsterTransferPlan]:
        self.last_plan = None
        if self._health()["state"] in {"blocked", "recovering"}:
            return None
        main, sub = self.snapshots.get("main"), self.snapshots.get("sub")
        if not main or not sub:
            return None
        self.last_plan = build_transfer_plan(main, sub, self.settings)
        return self.last_plan

    def _transfer_signature(self, params: List[Tuple[str, str]], private_key: str) -> str:
        try:
            from eth_account import Account
            from eth_account.messages import encode_typed_data
        except ImportError as exc:
            raise RuntimeError("eth-account is required for Aster sub-account transfers") from exc
        message_text = urlencode(params)
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [{"name": "msg", "type": "string"}],
            },
            "primaryType": "Message",
            "domain": {
                "name": "AsterSignTransaction",
                "version": "1",
                "chainId": 1666,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {"msg": message_text},
        }
        return Account.sign_message(encode_typed_data(full_message=typed_data), private_key).signature.hex()

    async def _submit_transfer(self, plan: AsterTransferPlan) -> Dict[str, Any]:
        main_address = self.settings.effective_master_wallet_address
        sub_address = self.settings.effective_sub_wallet_address
        if not main_address or not sub_address:
            raise RuntimeError("Aster main and sub wallet addresses are required for transfer")
        source_address = main_address if plan.source == "main" else sub_address
        destination_address = sub_address if plan.destination == "sub" else main_address
        nonce = max(int(time.time() * 1_000_000), int(getattr(self, "_last_transfer_nonce", 0)) + 1)
        self._last_transfer_nonce = nonce
        params: List[Tuple[str, str]] = [
            ("toAccountAddress", destination_address),
            ("asset", self.settings.asset),
            ("amount", _format_amount(plan.amount)),
            ("kindType", TRANSFER_KIND),
            ("nonce", str(nonce)),
            ("user", main_address),
        ]
        signing_key = self.settings.effective_transfer_signer_private_key
        signer_address = self.settings.effective_transfer_signer_address
        if signer_address:
            params.append(("signer", signer_address))
        if plan.source == "sub":
            params.append(("fromAccountAddress", source_address))
        params.append(("signature", self._transfer_signature(params, signing_key)))
        if self._session is None:
            raise RuntimeError("Aster HTTP session is not ready")
        timeout = aiohttp.ClientTimeout(total=self.settings.action_timeout_seconds)
        async with self._session.post(
            f"{ASTER_TRANSFER_REST_URL}/fapi/v3/subAccountTransfer",
            data=dict(params),
            timeout=timeout,
        ) as response:
            text = await response.text()
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Aster transfer returned invalid JSON: {text[:300]}") from exc
            if response.status != 200:
                raise RuntimeError(f"Aster transfer returned HTTP {response.status}: {text[:300]}")
            if isinstance(payload, Mapping) and payload.get("code") not in (None, 0, 200, "0", "200"):
                raise RuntimeError(f"Aster transfer rejected: {payload.get('code')} {payload.get('msg', '')}")
            return {"status": "accepted_pending_confirmation", "response": payload, "nonce": nonce}

    async def execute_transfer(self, *, request_id: Optional[str] = None) -> Dict[str, Any]:
        async with self._action_lock:
            self._transfer_allowed()
            before = {name: snapshot.available_balance for name, snapshot in self.snapshots.items()}
            plan = await self.calculate_transfer_plan()
            if plan is None:
                return {"status": "balanced", "plan": None}
            record = {"type": "transfer", "request_id": request_id or uuid.uuid4().hex, "plan": plan.as_payload(), "timestamp": time.time()}
            # Set the blocking intent before the network write. Aster documents
            # HTTP 503 as an unknown execution result, so every post-submit
            # exception must prevent an immediate retry.
            self.last_transfer = record
            self.transfer_history.append(record)
            self.transfer_history = self.transfer_history[-50:]
            self._pending_transfer = record
            self._last_transfer_at = time.time()
            self._persist_state()
            try:
                result = await self._submit_transfer(plan)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                record["result"] = {"status": "unknown_pending", "error": str(exc)}
                LOGGER.error("Aster transfer status is unknown; live transfers are blocked: %s", exc)
                self._persist_state()
                return _json_value(record)
            record["result"] = result
            self._persist_state()
            confirmed = await self._confirm_transfer(plan, before)
            if confirmed:
                record["result"] = {**result, "status": "acknowledged", "confirmed_at": time.time()}
                self._pending_transfer = None
                self._persist_state()
            return _json_value(record)

    async def _confirm_transfer(self, plan: AsterTransferPlan, before: Mapping[str, Decimal]) -> bool:
        for _ in range(3):
            await asyncio.sleep(0.5)
            try:
                await self.refresh_once()
            except Exception:
                continue
            source_delta = before[plan.source] - self.snapshots[plan.source].available_balance
            destination_delta = self.snapshots[plan.destination].available_balance - before[plan.destination]
            tolerance = max(Decimal("0.01"), plan.amount * Decimal("0.25"))
            if source_delta + tolerance >= plan.amount and destination_delta + tolerance >= plan.amount:
                return True
        return False

    async def manual_rebalance(self, *, request_id: Optional[str] = None) -> Dict[str, Any]:
        if not self.settings.live:
            await self.refresh_once()
            plan = await self.calculate_transfer_plan()
            return {"status": "dry_run", "plan": plan.as_payload() if plan else None}
        return await self.execute_transfer(request_id=request_id)

    async def _handle_dashboard_action(self, action: Any) -> Dict[str, Any]:
        if getattr(action, "action", "") == "rebalance":
            return await self.manual_rebalance(request_id=getattr(action, "request_id", None))
        raise RuntimeError("Aster neutral dashboard only supports rebalance")

    def snapshot_payload(self) -> Dict[str, Any]:
        main, sub = self.snapshots.get("main"), self.snapshots.get("sub")
        health = self._health()
        total_equity = main.equity + sub.equity if main and sub else None
        total_available = main.available_balance + sub.available_balance if main and sub else None
        available_ratio = total_available / total_equity if total_equity and total_equity > 0 else None
        return _json_value({
            "ok": bool(main and sub and not self._last_refresh_error),
            "state": "healthy" if main and sub and not self._last_refresh_error else "degraded",
            "exchange": "aster",
            "symbol": self.settings.symbol,
            "live": self.settings.live,
            "auto_transfer": self.settings.auto_transfer,
            "monitor_only": not self.settings.live,
            "alert_threshold": self.settings.alert_threshold,
            "dashboard_actions_enabled": bool(self.settings.live and self.settings.dashboard_token),
            "last_refresh_error": self._last_refresh_error,
            "transfer_state": health["state"],
            "transfer_allowed": bool(self.settings.live and health["allowed"]),
            "transfer_health": health,
            "accounts": {name: snapshot.as_payload() for name, snapshot in self.snapshots.items()},
            "aggregate": {
                "total_equity": total_equity,
                "total_available_balance": total_available,
                "available_balance_delta": main.available_balance - sub.available_balance if main and sub else None,
                "available_balance_delta_abs": abs(main.available_balance - sub.available_balance) if main and sub else None,
                "available_balance_to_total_equity_ratio": available_ratio,
                "transfer_hysteresis": self.settings.transfer_hysteresis,
                "transfer_trigger_threshold": max(
                    self.settings.transfer_hysteresis,
                    self.settings.min_transfer * Decimal("2"),
                ),
            },
            "transfer_plan": self.last_plan.as_payload() if self.last_plan else None,
            "last_transfer": self.last_transfer,
            "transfer_history": list(reversed(self.transfer_history[-20:])),
            "pending_transfer": self._pending_transfer,
            "updated_at": time.time(),
        })

    @staticmethod
    def _value(value: Any) -> str:
        if value is None:
            return "-"
        return str(value)

    def _feishu_text(self) -> str:
        payload = self.snapshot_payload()
        aggregate = payload["aggregate"]
        lines = [
            "Aster XAUUSD1 中性账户报告",
            time.strftime("时间: %Y-%m-%d %H:%M:%S %Z", time.localtime()),
            f"状态: {payload['state']} | 转账状态: {payload['transfer_state']} | 允许转账: {'是' if payload['transfer_allowed'] else '否'}",
            f"两账户总权益: {self._value(aggregate['total_equity'])} {self.settings.asset} | 可用保证金总额: {self._value(aggregate['total_available_balance'])} {self.settings.asset}",
            f"可用保证金差值(主-子): {self._value(aggregate['available_balance_delta'])} {self.settings.asset}",
        ]
        for name, label in (("main", "主账户"), ("sub", "子账户")):
            account = payload["accounts"].get(name, {})
            position = account.get("position") or {}
            lines.append(
                f"{label}: 权益 {self._value(account.get('equity'))} | 可用 {self._value(account.get('available_balance'))} | "
                f"可转出 {self._value(account.get('max_withdraw_amount'))} | 维持保证金 {self._value(account.get('maintenance_margin'))}"
            )
            lines.append(
                f"  {self.settings.symbol}: 仓位 {self._value(position.get('signed_size'))} | 名义 {self._value(position.get('position_value'))} | "
                f"未实现盈亏 {self._value(position.get('unrealized_pnl'))}"
            )
        reason = payload["transfer_health"].get("reason")
        if reason:
            lines.append(f"转账限制原因: {reason}")
        return "\n".join(lines)

    async def _maybe_feishu(self) -> None:
        if not self.settings.feishu_webhook_url or self._session is None:
            return
        main = self.snapshots.get("main")
        sub = self.snapshots.get("sub")
        delta = abs(main.available_balance - sub.available_balance) if main and sub and not main.error and not sub.error else None
        should_alert = delta is not None and delta >= self.settings.alert_threshold
        if self._last_refresh_error or (main and main.error) or (sub and sub.error):
            should_alert = True
        if not should_alert:
            return
        now = time.monotonic()
        if now < self._next_feishu_report:
            return
        self._next_feishu_report = now + self.settings.feishu_interval_seconds
        body = {"msg_type": "text", "content": {"text": self._feishu_text()}}
        if self.settings.feishu_webhook_secret:
            timestamp = str(int(time.time()))
            sign_source = f"{timestamp}\n{self.settings.feishu_webhook_secret}".encode()
            digest = hmac.new(self.settings.feishu_webhook_secret.encode(), sign_source, hashlib.sha256).digest()
            body.update({"timestamp": timestamp, "sign": base64.b64encode(digest).decode()})
        try:
            async with self._session.post(self.settings.feishu_webhook_url, json=body, timeout=aiohttp.ClientTimeout(total=10)) as response:
                text = await response.text()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}: {text[:200]}")
            LOGGER.info("Aster neutral Feishu report sent")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("Aster neutral Feishu report failed: %s", exc)

    async def run(self) -> None:
        if self._session is None:
            await self.start()
        while not self._stop_event.is_set():
            try:
                await self.refresh_once()
                self.last_plan = await self.calculate_transfer_plan()
                if self.last_plan and self.settings.auto_transfer and self.settings.live and self._health()["allowed"]:
                    await self.execute_transfer()
                await self._maybe_feishu()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_refresh_error = str(exc)
                LOGGER.exception("Aster neutral monitor iteration failed")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.poll_seconds)
            except asyncio.TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop_event.set()
        if self._dashboard is not None:
            with contextlib.suppress(Exception):
                await self._dashboard.stop()
            self._dashboard = None
        for client in self._clients.values():
            with contextlib.suppress(Exception):
                await client.close()
        if self._session is not None:
            with contextlib.suppress(Exception):
                await self._session.close()
            self._session = None
        self._instance_lock.release()


def settings_from_env(env_file: Optional[str] = None) -> AsterNeutralSettings:
    values: Dict[str, Any] = dict(os.environ)
    if env_file:
        values.update({key: value for key, value in dotenv.dotenv_values(env_file).items() if value is not None})
    main = AsterAccountSpec(
        "main",
        str(values.get("ASTER_NEUTRAL_MAIN_API_KEY", "") or ""),
        str(values.get("ASTER_NEUTRAL_MAIN_API_SECRET", "") or ""),
        str(values.get("ASTER_NEUTRAL_MAIN_WALLET_ADDRESS", "") or ""),
        str(values.get("ASTER_NEUTRAL_MAIN_USER_ADDRESS", "") or ""),
        str(values.get("ASTER_NEUTRAL_MAIN_SIGNER_ADDRESS", "") or ""),
        str(values.get("ASTER_NEUTRAL_MAIN_SIGNER_PRIVATE_KEY", "") or ""),
    )
    sub = AsterAccountSpec(
        "sub",
        str(values.get("ASTER_NEUTRAL_SUB_API_KEY", "") or ""),
        str(values.get("ASTER_NEUTRAL_SUB_API_SECRET", "") or ""),
        str(values.get("ASTER_NEUTRAL_SUB_WALLET_ADDRESS", "") or ""),
        str(values.get("ASTER_NEUTRAL_SUB_USER_ADDRESS", "") or ""),
        str(values.get("ASTER_NEUTRAL_SUB_SIGNER_ADDRESS", "") or ""),
        str(values.get("ASTER_NEUTRAL_SUB_SIGNER_PRIVATE_KEY", "") or ""),
    )
    return AsterNeutralSettings(
        main=main,
        sub=sub,
        symbol=str(values.get("ASTER_NEUTRAL_SYMBOL", "XAUUSD1") or "XAUUSD1"),
        asset=str(values.get("ASTER_NEUTRAL_ASSET", TRANSFER_ASSET) or TRANSFER_ASSET),
        rest_url=str(values.get("ASTER_NEUTRAL_REST_URL", ASTER_REST_URL) or ASTER_REST_URL).rstrip("/"),
        poll_seconds=float(values.get("ASTER_NEUTRAL_POLL_SECONDS", "5")),
        snapshot_max_age_seconds=float(values.get("ASTER_NEUTRAL_SNAPSHOT_MAX_AGE_SECONDS", "15")),
        recovery_successes_required=_env_int("ASTER_NEUTRAL_RECOVERY_SUCCESSES", 3, values),
        transfer_hysteresis=_required_decimal(values.get("ASTER_NEUTRAL_TRANSFER_HYSTERESIS", "10"), "transfer hysteresis"),
        min_transfer=_required_decimal(values.get("ASTER_NEUTRAL_MIN_TRANSFER", "1"), "minimum transfer"),
        max_transfer=_required_decimal(values.get("ASTER_NEUTRAL_MAX_TRANSFER", "1000"), "maximum transfer"),
        transfer_cooldown_seconds=float(values.get("ASTER_NEUTRAL_TRANSFER_COOLDOWN_SECONDS", "30")),
        live=_env_bool(values.get("ASTER_NEUTRAL_LIVE"), False) and _env_bool(values.get("ASTER_NEUTRAL_ENABLE_TRANSFERS"), False),
        auto_transfer=_env_bool(values.get("ASTER_NEUTRAL_AUTO_TRANSFER"), False) and _env_bool(values.get("ASTER_NEUTRAL_ENABLE_TRANSFERS"), False),
        transfers_enabled=_env_bool(values.get("ASTER_NEUTRAL_ENABLE_TRANSFERS"), False),
        wallet_private_key=str(values.get("ASTER_NEUTRAL_WALLET_PRIVATE_KEY", "") or ""),
        wallet_address=str(values.get("ASTER_NEUTRAL_MASTER_WALLET_ADDRESS", values.get("ASTER_NEUTRAL_MAIN_USER_ADDRESS", "")) or ""),
        transfer_signer_address=str(values.get("ASTER_NEUTRAL_TRANSFER_SIGNER_ADDRESS", "") or ""),
        transfer_signer_private_key=str(values.get("ASTER_NEUTRAL_TRANSFER_SIGNER_PRIVATE_KEY", "") or ""),
        dashboard_host=str(values.get("ASTER_NEUTRAL_DASHBOARD_HOST", "127.0.0.1")),
        dashboard_port=_env_int("ASTER_NEUTRAL_DASHBOARD_PORT", 8791, values),
        dashboard_username=str(values.get("ASTER_NEUTRAL_DASHBOARD_USERNAME", "operator")),
        dashboard_token=str(values.get("ASTER_NEUTRAL_DASHBOARD_TOKEN", "") or ""),
        dashboard_allow_public_bind=_env_bool(values.get("ASTER_NEUTRAL_DASHBOARD_ALLOW_PUBLIC"), False),
        feishu_webhook_url=str(values.get("ASTER_NEUTRAL_FEISHU_WEBHOOK_URL", "") or "").strip(),
        feishu_webhook_secret=str(values.get("ASTER_NEUTRAL_FEISHU_WEBHOOK_SECRET", "") or "").strip(),
        feishu_interval_seconds=float(values.get("ASTER_NEUTRAL_FEISHU_INTERVAL_SECONDS", "600")),
        alert_threshold=_required_decimal(values.get("ASTER_NEUTRAL_ALERT_THRESHOLD", "100"), "alert threshold"),
        request_timeout_seconds=float(values.get("ASTER_NEUTRAL_REQUEST_TIMEOUT_SECONDS", "10")),
        action_timeout_seconds=float(values.get("ASTER_NEUTRAL_ACTION_TIMEOUT_SECONDS", "20")),
        state_path=str(values.get("ASTER_NEUTRAL_STATE_PATH", "logs/aster_neutral_manager_state.json")),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor two Aster XAUUSD1 neutral accounts")
    parser.add_argument("--env-file")
    parser.add_argument("--symbol")
    parser.add_argument("--main-api-key")
    parser.add_argument("--main-api-secret")
    parser.add_argument("--sub-api-key")
    parser.add_argument("--sub-api-secret")
    parser.add_argument("--poll-seconds", type=float)
    parser.add_argument("--port", type=int)
    parser.add_argument("--dashboard-token")
    parser.add_argument("--feishu-webhook-url")
    parser.add_argument("--feishu-webhook-secret")
    return parser


async def _run(settings: AsterNeutralSettings) -> None:
    manager = AsterNeutralManager(settings)
    await manager.start()
    loop = asyncio.get_running_loop()
    handlers = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, manager._stop_event.set)
            handlers.append(signum)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        await manager.run()
    finally:
        for signum in handlers:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(signum)
        await manager.stop()


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    settings = settings_from_env(args.env_file)
    for attr, value in (("symbol", args.symbol), ("poll_seconds", args.poll_seconds), ("dashboard_port", args.port),
                        ("dashboard_token", args.dashboard_token), ("feishu_webhook_url", args.feishu_webhook_url),
                        ("feishu_webhook_secret", args.feishu_webhook_secret)):
        if value is not None:
            setattr(settings, attr, value)
    for name, value in (("main", args.main_api_key), ("sub", args.sub_api_key)):
        if value is not None:
            spec = getattr(settings, name)
            setattr(settings, name, AsterAccountSpec(spec.name, value, spec.api_secret, spec.wallet_address))
    for name, value in (("main", args.main_api_secret), ("sub", args.sub_api_secret)):
        if value is not None:
            spec = getattr(settings, name)
            setattr(settings, name, AsterAccountSpec(spec.name, spec.api_key, value, spec.wallet_address))
    settings.validate()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(_run(settings))
    except KeyboardInterrupt:
        LOGGER.info("Aster neutral manager stopped")


if __name__ == "__main__":
    main()
