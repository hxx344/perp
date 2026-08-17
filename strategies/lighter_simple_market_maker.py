"""Minimal Robinhood Lighter market-making loop.

This module keeps a single post-only bid/ask resting on Lighter, uses the public
Binance Futures order book as a relative pressure signal when available, and
optionally hedges net exposure on Binance Futures once an inventory threshold
is breached. Binance trading is deliberately opt-in; the normal Robinhood
deployment only reads public Binance depth. Hot-update configuration is
reloaded each loop iteration so ops can pause the cycle or tweak parameters
without restarts.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import ctypes
import hmac
import json
import logging
import os
import random
import re
import secrets
import signal
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional, cast
from urllib.parse import urlencode

import aiohttp
import dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


DEFAULT_ENV_CANDIDATES: tuple[str, ...] = (
    "/etc/perp/robinhood.env",
    "robinhood.env",
    ".env.robinhood",
    ".env",
)
LIGHTER_ENDPOINT_ENV_KEYS: tuple[str, ...] = (
    "LIGHTER_ENVIRONMENT",
    "LIGHTER_ENDPOINT_PROFILE",
    "LIGHTER_BASE_URL",
    "LIGHTER_WS_URL",
    "LIGHTER_CHAIN_ID",
)
LIGHTER_CREDENTIAL_ENV_KEYS: tuple[str, ...] = (
    "LIGHTER_ACCOUNT_INDEX",
    "LIGHTER_API_PRIVATE_KEYS",
    "API_KEY_PRIVATE_KEYS",
    "API_KEY_PRIVATE_KEY",
    "LIGHTER_API_KEY_INDEX",
    "L1_WALLET_PRIVATE_KEY",
    "LIGHTER_L1_PRIVATE_KEY",
)

from exchanges import ExchangeFactory
from exchanges.lighter import LighterClient
from helpers.logger import TradingLogger
from trading_bot import TradingConfig


_LOGGER = logging.getLogger(__name__)


def _decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except Exception as exc:  # pragma: no cover - defensive parsing guard
        raise argparse.ArgumentTypeError(f"Invalid decimal value '{value}': {exc}") from exc


@dataclass(slots=True)
class SimpleMakerSettings:
    """Runtime configuration for the simple market maker."""

    lighter_ticker: str
    binance_symbol: str
    order_quantity: Decimal
    base_spread_bps: Decimal
    hedge_threshold: Decimal
    hedge_buffer: Decimal = Decimal("0")
    enable_binance_hedge: bool = False
    hedge_cooldown_seconds: float = 0.0
    max_hedge_quantity: Optional[Decimal] = None
    inventory_limit: Optional[Decimal] = None
    config_path: str = "configs/hot_update.json"
    env_file: Optional[str] = None
    lighter_environment: str = "robinhood"
    lighter_leverage: Optional[int] = 2
    loop_sleep_seconds: float = 3.0
    order_refresh_ticks: int = 2
    order_refresh_bps: Decimal = Decimal("1")
    min_quote_lifetime_seconds: float = 5.0
    order_ack_timeout_seconds: float = 5.0
    shutdown_timeout_seconds: float = 10.0
    binance_reference_timeout_seconds: float = 1.0
    binance_depth_levels: int = 10
    binance_imbalance_max_bps: Decimal = Decimal("3")
    bbo_max_distance_ticks: int = 1
    max_cycles: int = 0
    log_to_console: bool = True
    metrics_interval_seconds: float = 30.0
    allowed_sides: Optional[Iterable[str]] = None
    order_quantity_min: Optional[Decimal] = None
    order_quantity_max: Optional[Decimal] = None
    fill_cooldown_seconds: float = 5.0
    use_binance_reference: bool = True
    inventory_skew_bps: Decimal = Decimal("3")
    ownership_state_path: Optional[str] = None
    allow_existing_binance_position: bool = False

    def effective_inventory_limit(self) -> Decimal:
        return self.inventory_limit if self.inventory_limit is not None else self.hedge_threshold


@dataclass(slots=True)
class ActiveOrder:
    """Tracked order metadata for a single side."""

    order_id: str
    price: Decimal
    side: str
    client_order_index: Optional[str] = None
    created_at: float = 0.0
    confirmed: bool = False
    missing_active_snapshots: int = 0


def resolve_default_env_file() -> str:
    """Return the first readable Robinhood environment file.

    The deployment credential file is preferred over repository-local files so
    a stale ``.env`` cannot silently select another Lighter account.
    """

    for candidate in DEFAULT_ENV_CANDIDATES:
        path = Path(candidate)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        try:
            if path.is_file() and os.access(path, os.R_OK):
                return candidate
        except OSError:
            continue
    return DEFAULT_ENV_CANDIDATES[0]


def compute_target_prices(
    mid_price: Decimal,
    spread_bps: Decimal,
    tick_size: Decimal,
) -> Dict[str, Decimal]:
    """Return rounded bid/ask targets based on mid price and spread."""
    half_spread = (mid_price * spread_bps / Decimal("10000")).quantize(tick_size, rounding=ROUND_HALF_UP)
    if half_spread < tick_size:
        half_spread = tick_size
    # Directional rounding keeps the bid at or below its intended level and
    # the ask at or above it. This matters when the reference is Binance and
    # the local Robinhood book is one tick away from the lead price.
    bid_price = (mid_price - half_spread).quantize(tick_size, rounding=ROUND_DOWN)
    ask_price = (mid_price + half_spread).quantize(tick_size, rounding=ROUND_UP)
    return {"buy": bid_price, "sell": ask_price}


def compute_orderbook_imbalance(
    bids: Iterable[Any],
    asks: Iterable[Any],
    depth_levels: int = 10,
) -> Decimal:
    """Return a dimensionless bid/ask pressure score in ``[-1, 1]``.

    Only relative displayed size is used. This intentionally avoids importing
    Binance's absolute price into Lighter pricing when the two venues use
    different quote assets, contract denominations, or bases. Nearer levels
    receive more weight so a large distant wall cannot dominate the signal.
    """

    if depth_levels <= 0:
        raise ValueError("depth_levels must be positive")

    def _score(levels: Iterable[Any]) -> Decimal:
        score = Decimal("0")
        for index, level in enumerate(levels):
            if index >= depth_levels:
                break
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            try:
                price = Decimal(str(level[0]))
                quantity = Decimal(str(level[1]))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if not price.is_finite() or not quantity.is_finite() or price <= 0 or quantity <= 0:
                continue
            score += quantity / Decimal(index + 1)
        return score

    bid_score = _score(bids)
    ask_score = _score(asks)
    total = bid_score + ask_score
    if bid_score <= 0 or ask_score <= 0 or total <= 0:
        raise ValueError("Binance order book has no usable bid/ask depth")
    imbalance = (bid_score - ask_score) / total
    return max(Decimal("-1"), min(Decimal("1"), imbalance))


def apply_orderbook_imbalance(
    local_mid: Decimal,
    imbalance: Decimal,
    max_offset_bps: Decimal,
) -> Decimal:
    """Shift a local Lighter midpoint by a bounded relative pressure signal."""

    if local_mid <= 0 or max_offset_bps <= 0:
        return local_mid
    if not imbalance.is_finite():
        return local_mid
    bounded = max(Decimal("-1"), min(Decimal("1"), imbalance))
    relative_offset = bounded * max_offset_bps / Decimal("10000")
    return local_mid * (Decimal("1") + relative_offset)


def should_enable_side(net_position: Decimal, limit: Decimal, side: str) -> bool:
    """Check if quoting for a side should remain enabled under inventory constraints."""
    if side == "buy":
        return net_position < limit
    return net_position > -limit


def side_has_inventory_capacity(
    net_position: Decimal,
    limit: Decimal,
    side: str,
    order_quantity: Decimal,
) -> bool:
    """Ensure a full next quote cannot take inventory through the hard cap."""

    quantity = abs(order_quantity)
    if quantity <= 0:
        return should_enable_side(net_position, limit, side)
    if side == "buy":
        return net_position + quantity < limit
    if side == "sell":
        return net_position - quantity > -limit
    raise ValueError(f"Invalid side: {side!r}")


def apply_inventory_skew(
    reference_mid: Decimal,
    net_position: Decimal,
    inventory_limit: Decimal,
    max_skew_bps: Decimal,
) -> Decimal:
    """Shift the quote center to encourage inventory back toward zero.

    A long inventory moves both quotes down (less aggressive bid, more
    aggressive ask); a short inventory moves them up. The adjustment is linear
    and capped at ``max_skew_bps`` when the hard inventory limit is reached.
    """

    if reference_mid <= 0 or inventory_limit <= 0 or max_skew_bps <= 0:
        return reference_mid
    ratio = net_position / inventory_limit
    ratio = max(Decimal("-1"), min(Decimal("1"), ratio))
    adjustment = reference_mid * max_skew_bps * ratio / Decimal("10000")
    return reference_mid - adjustment


def clamp_maker_targets(
    targets: Dict[str, Decimal],
    best_bid: Decimal,
    best_ask: Decimal,
    tick_size: Decimal,
    *,
    max_bbo_distance_ticks: int = 1,
) -> Dict[str, Decimal]:
    """Keep post-only targets at or immediately inside Lighter depth 1."""

    if tick_size <= 0:
        return {"buy": best_bid, "sell": best_ask}

    distance_ticks = max(0, int(max_bbo_distance_ticks))
    # The reference signal may improve depth 1 by at most the configured
    # distance. This keeps Binance pressure and inventory skew useful without
    # allowing either signal to move a quote deep into the spread.
    max_post_only_bid = min(
        best_ask - tick_size,
        best_bid + (tick_size * distance_ticks),
    )
    min_post_only_ask = max(
        best_bid + tick_size,
        best_ask - (tick_size * distance_ticks),
    )
    bid = max(best_bid, min(targets["buy"], max_post_only_bid))
    ask = min(best_ask, max(targets["sell"], min_post_only_ask))
    if bid >= ask:
        # A stale or dislocated reference should never create a crossed pair.
        bid = best_bid
        ask = best_ask
    bid = bid.quantize(tick_size, rounding=ROUND_DOWN)
    ask = ask.quantize(tick_size, rounding=ROUND_UP)
    return {"buy": bid, "sell": ask}


def required_hedge_quantity(net_position: Decimal, threshold: Decimal, buffer: Decimal) -> Decimal:
    """Calculate the hedge amount when exposure exceeds the threshold."""
    exposure = abs(net_position)
    if exposure < threshold:
        return Decimal("0")
    hedge_qty = exposure - buffer
    if hedge_qty <= 0:
        return Decimal("0")
    return hedge_qty


class BinanceHedger:
    """Minimal REST client for Binance USDT-margined futures hedging."""

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str, symbol: str, session: aiohttp.ClientSession) -> None:
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.symbol = symbol.upper()
        self.session = session
        self._quantity_step: Optional[Decimal] = None
        self._min_quantity: Optional[Decimal] = None

    def _sign(self, params: Dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        signature = hmac.new(self.api_secret, query.encode(), sha256).hexdigest()
        return signature

    async def place_market_order(
        self,
        side: str,
        quantity: Decimal,
        *,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        normalized_qty = await self.prepare_market_quantity(quantity)
        if normalized_qty <= 0:
            raise ValueError(
                f"Binance order quantity below minimum lot size after normalization: requested={quantity}"
            )
        timestamp = int(time.time() * 1000)
        params = {
            "symbol": self.symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": format(normalized_qty, "f"),
            "timestamp": timestamp,
            "recvWindow": 5000,
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        params["signature"] = self._sign(params)
        headers = {"X-MBX-APIKEY": self.api_key}

        async with self.session.post(f"{self.BASE_URL}/fapi/v1/order", params=params, headers=headers) as response:
            data = await response.json()
            if response.status >= 400:
                raise RuntimeError(f"Binance order failed: {response.status} {data}")
            return data

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    async def get_account_metrics(self) -> Dict[str, Decimal]:
        """Return wallet balances, position size and PnL for the configured symbol."""
        timestamp = int(time.time() * 1000)
        params: Dict[str, Any] = {
            "timestamp": timestamp,
            "recvWindow": 5000,
        }
        params["signature"] = self._sign(params)
        headers = {"X-MBX-APIKEY": self.api_key}

        async with self.session.get(f"{self.BASE_URL}/fapi/v2/account", params=params, headers=headers) as response:
            data = await response.json()
            if response.status >= 400:
                raise RuntimeError(f"Binance account metrics failed: {response.status} {data}")

        positions = data.get("positions", []) if isinstance(data, dict) else []
        target_position = next((pos for pos in positions if pos.get("symbol") == self.symbol), {})

        metrics: Dict[str, Decimal] = {
            "wallet_balance": self._to_decimal(data.get("totalWalletBalance")),
            "available_balance": self._to_decimal(data.get("availableBalance", data.get("maxWithdrawAmount"))),
            "unrealized_pnl": self._to_decimal(data.get("totalUnrealizedProfit")),
            "position_size": self._to_decimal(target_position.get("positionAmt")),
            "position_notional": self._to_decimal(target_position.get("notional")),
            "position_unrealized_pnl": self._to_decimal(target_position.get("unrealizedProfit")),
            "position_entry_price": self._to_decimal(target_position.get("entryPrice")),
        }

        return metrics

    async def _ensure_symbol_filters(self) -> None:
        if self._quantity_step is not None and self._min_quantity is not None:
            return

        params = {"symbol": self.symbol}
        async with self.session.get(f"{self.BASE_URL}/fapi/v1/exchangeInfo", params=params) as response:
            data = await response.json()
            if response.status >= 400:
                raise RuntimeError(
                    f"Failed to load Binance symbol metadata: {response.status} {data}"
                )

        symbol_info: Optional[Dict[str, Any]] = None
        if isinstance(data, dict):
            symbols = data.get("symbols") or []
            for entry in symbols:
                if isinstance(entry, dict) and entry.get("symbol") == self.symbol:
                    symbol_info = entry
                    break

        if not symbol_info:
            raise RuntimeError(f"Symbol metadata for '{self.symbol}' not found in Binance exchangeInfo response")

        filters = symbol_info.get("filters") or []
        lot_filter = next(
            (f for f in filters if isinstance(f, dict) and f.get("filterType") == "MARKET_LOT_SIZE"),
            None,
        )
        if lot_filter is None:
            lot_filter = next(
                (f for f in filters if isinstance(f, dict) and f.get("filterType") == "LOT_SIZE"),
                None,
            )

        if isinstance(lot_filter, dict):
            step = self._to_decimal(lot_filter.get("stepSize"))
            min_qty = self._to_decimal(lot_filter.get("minQty"))
            if step > 0:
                self._quantity_step = step
            if min_qty > 0:
                self._min_quantity = min_qty

    @staticmethod
    def _round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        quotient = (value / step).to_integral_value(rounding=ROUND_DOWN)
        rounded = (quotient * step).quantize(step, rounding=ROUND_DOWN)
        return rounded

    async def prepare_market_quantity(self, quantity: Decimal) -> Decimal:
        if quantity <= 0:
            return Decimal("0")

        await self._ensure_symbol_filters()

        normalized = quantity
        if self._quantity_step is not None:
            normalized = self._round_down_to_step(quantity, self._quantity_step)

        if self._min_quantity is not None and normalized < self._min_quantity:
            return Decimal("0")

        return normalized

    def lot_size_constraints(self) -> Dict[str, Optional[Decimal]]:
        return {"step_size": self._quantity_step, "min_quantity": self._min_quantity}


class BinancePublicReference:
    """Read-only Binance depth source used as a relative pressure signal."""

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, symbol: str, session: aiohttp.ClientSession) -> None:
        self.symbol = symbol.upper()
        self.session = session

    async def fetch_mid_price(self) -> Decimal:
        """Return Binance Futures best-bid/ask midpoint without credentials.

        Kept for compatibility with callers outside this strategy. The maker
        loop deliberately does not use this absolute price as its Lighter
        quote center.
        """

        async with self.session.get(
            f"{self.BASE_URL}/fapi/v1/ticker/bookTicker",
            params={"symbol": self.symbol},
        ) as response:
            data = await response.json()
            if response.status >= 400:
                raise RuntimeError(f"Binance public book ticker failed: {response.status} {data}")

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected Binance public book ticker response: {data!r}")
        try:
            bid = Decimal(str(data.get("bidPrice", "0")))
            ask = Decimal(str(data.get("askPrice", "0")))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid Binance public book ticker response: {data!r}") from exc
        if bid <= 0 or ask <= 0 or bid >= ask:
            raise RuntimeError(f"Invalid Binance public book ticker prices: bid={bid} ask={ask}")
        return (bid + ask) / Decimal("2")

    async def fetch_orderbook_imbalance(self, depth_levels: int = 10) -> Decimal:
        """Return weighted Binance bid/ask depth pressure without credentials."""

        if depth_levels not in (5, 10, 20, 50, 100, 500, 1000):
            raise ValueError(
                "Binance depth limit must be one of 5, 10, 20, 50, 100, 500, or 1000"
            )

        async with self.session.get(
            f"{self.BASE_URL}/fapi/v1/depth",
            params={"symbol": self.symbol, "limit": depth_levels},
        ) as response:
            data = await response.json()
            if response.status >= 400:
                raise RuntimeError(f"Binance public depth failed: {response.status} {data}")

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected Binance public depth response: {data!r}")
        return compute_orderbook_imbalance(
            data.get("bids") or [],
            data.get("asks") or [],
            depth_levels=depth_levels,
        )


class SimpleMarketMaker:
    """Run a lightweight post-only maker loop on Robinhood Lighter."""

    def __init__(self, settings: SimpleMakerSettings) -> None:
        self.settings = settings
        self.logger = TradingLogger("lighter-simple", settings.lighter_ticker, log_to_console=settings.log_to_console)
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._hedger: Optional[BinanceHedger] = None
        self._binance_reference: Optional[BinancePublicReference] = None
        self._lighter_client: Optional[LighterClient] = None
        self._lighter_config: Optional[TradingConfig] = None
        self._tracked_orders: Dict[str, ActiveOrder] = {}
        # The REST active-order endpoint returns the whole account. Keep a
        # client-id registry so this process never cancels a manual order or a
        # quote owned by another strategy.
        self._own_client_order_indices: set[str] = set()
        self._last_hot_update: Dict[str, Any] = {}
        self._last_metrics_time: float = 0.0
        self._lighter_order_fills: Dict[str, Decimal] = {}
        self._lighter_session_volume_quote: Decimal = Decimal("0")
        self._lighter_session_volume_base: Decimal = Decimal("0")
        self._binance_position_estimate: Decimal = Decimal("0")
        self._binance_state_known = not settings.enable_binance_hedge
        self._last_hedge_timestamp = 0.0
        self._binance_initial_wallet_balance: Optional[Decimal] = None
        self._base_rate_limit_backoff_seconds = max(float(self.settings.loop_sleep_seconds), 1.0)
        self._rate_limit_backoff_seconds = self._base_rate_limit_backoff_seconds
        self._max_rate_limit_backoff_seconds = 60.0
        self._lighter_inventory_base: Decimal = Decimal("0")
        self._lighter_avg_entry_price: Decimal = Decimal("0")
        self._lighter_session_realized_pnl: Decimal = Decimal("0")
        self._lighter_last_mark_price: Decimal = Decimal("0")
        self._state_task: Optional[asyncio.Task] = None
        self._state_refresh_interval = max(1.0, min(float(self.settings.metrics_interval_seconds), 5.0))
        self._latest_metrics: Dict[str, Decimal] = {}
        self._latest_net_position: Decimal = Decimal("0")
        self._latest_net_position_time: float = 0.0
        self._inventory_state_known = False
        self._state_update_lock = asyncio.Lock()
        self._quote_operation_lock = asyncio.Lock()
        self._binance_session_realized_pnl: Decimal = Decimal("0")
        self._binance_inventory_base: Decimal = Decimal("0")
        self._binance_avg_entry_price: Decimal = Decimal("0")
        self._binance_last_mark_price: Decimal = Decimal("0")
        self._binance_session_volume_quote: Decimal = Decimal("0")
        self._binance_session_volume_base: Decimal = Decimal("0")
        self._allowed_sides = self._normalize_allowed_sides(settings.allowed_sides)
        self._external_pause = False
        self._pause_enforced = False
        self._rng = random.Random()
        self._quantity_step = self._derive_quantity_step(settings.order_quantity)
        self._dynamic_quantity_range = self._initialize_quantity_range(settings)
        self._flatten_lock = asyncio.Lock()
        self._flatten_active = False
        self._fill_cooldown_seconds = max(0.0, float(settings.fill_cooldown_seconds))
        self._last_fill_timestamp: Dict[str, float] = {"buy": 0.0, "sell": 0.0}
        self._stop_completed = False
        self._stop_error: Optional[RuntimeError] = None
        self._ownership_state_path: Optional[Path] = None
        self._instance_lock_path: Optional[Path] = None
        self._instance_lock_fd: Optional[int] = None
        self._instance_lock_token = ""

    async def __aenter__(self) -> "SimpleMarketMaker":
        try:
            await self.start()
        except Exception:
            await self.stop()
            raise
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._running:
            return

        env_path = self.settings.env_file or resolve_default_env_file()
        # Endpoint and chain settings must come from the selected profile. Clear
        # inherited values first so a shell used for Core cannot redirect this
        # Robinhood strategy. When a protected file exists, its credentials are
        # authoritative as well; a partial file must not silently inherit keys
        # from an unrelated shell session.
        for key in LIGHTER_ENDPOINT_ENV_KEYS:
            os.environ.pop(key, None)
        env_path_obj = Path(env_path)
        if not env_path_obj.is_absolute():
            env_path_obj = PROJECT_ROOT / env_path_obj
        if env_path_obj.is_file():
            for key in LIGHTER_CREDENTIAL_ENV_KEYS:
                os.environ.pop(key, None)
            loaded = dotenv.load_dotenv(str(env_path_obj), override=True)
            if loaded:
                self.logger.log(f"Loaded environment variables from '{env_path_obj}'", "INFO")
            else:
                self.logger.log(
                    f"Env file '{env_path_obj}' is empty; using existing credential environment",
                    "WARNING",
                )
        else:
            self.logger.log(
                f"Env file '{env_path_obj}' not found; using existing credential environment",
                "WARNING",
            )

        lighter_environment = (self.settings.lighter_environment or "robinhood").strip().lower()
        if lighter_environment != "robinhood":
            raise ValueError(
                "lighter_simple_market_maker is restricted to the Robinhood Lighter profile; "
                "use strategies.aster_lighter_cycle for Core Lighter"
            )
        os.environ["LIGHTER_ENVIRONMENT"] = "robinhood"

        timeout = aiohttp.ClientTimeout(total=15)
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._binance_reference = BinancePublicReference(self.settings.binance_symbol, self._session)

        initial_binance_position = Decimal("0")
        initial_binance_avg_price = Decimal("0")
        initial_binance_mark = Decimal("0")

        if self.settings.enable_binance_hedge:
            api_key = self._require_env("BINANCE_API_KEY")
            api_secret = self._require_env("BINANCE_API_SECRET")
            self._hedger = BinanceHedger(api_key, api_secret, self.settings.binance_symbol, self._session)

            try:
                hedger_snapshot = await self._hedger.get_account_metrics()
                position_size = hedger_snapshot.get("position_size", Decimal("0"))
                position_notional = hedger_snapshot.get("position_notional", Decimal("0"))
                position_entry_price = hedger_snapshot.get("position_entry_price", Decimal("0"))
                position_unrealized = hedger_snapshot.get("position_unrealized_pnl", Decimal("0"))
                self._binance_position_estimate = position_size
                initial_binance_position = position_size
                if position_size != 0 and not self.settings.allow_existing_binance_position:
                    raise RuntimeError(
                        "Binance hedge account already has a position; use a dedicated clean "
                        "hedge account or explicitly pass --allow-existing-binance-position"
                    )
                if position_size != 0 and position_notional != 0:
                    try:
                        initial_binance_avg_price = abs(position_notional) / abs(position_size)
                        initial_binance_mark = initial_binance_avg_price
                    except (InvalidOperation, ZeroDivisionError):
                        initial_binance_avg_price = Decimal("0")
                        initial_binance_mark = Decimal("0")
                if position_size != 0 and position_entry_price > 0:
                    initial_binance_avg_price = position_entry_price
                    try:
                        mark_candidate = position_entry_price + (position_unrealized / position_size)
                        if mark_candidate > 0:
                            initial_binance_mark = mark_candidate
                    except (InvalidOperation, ZeroDivisionError):
                        pass
                wallet_balance = hedger_snapshot.get("wallet_balance")
                if wallet_balance is not None:
                    self._binance_initial_wallet_balance = wallet_balance
            except Exception as exc:  # pragma: no cover - network dependent
                self._binance_state_known = False
                raise RuntimeError(
                    f"Binance hedge was enabled but its account position could not be reconciled: {exc}"
                ) from exc
            self._binance_state_known = True
        else:
            self._hedger = None
            self._binance_position_estimate = Decimal("0")
            self._binance_initial_wallet_balance = None
            self.logger.log("Binance hedging disabled; skipping external hedge initialization", "INFO")

        trading_config = TradingConfig(
            ticker=self.settings.lighter_ticker,
            contract_id="",
            quantity=self.settings.order_quantity,
            take_profit=Decimal("0"),
            tick_size=Decimal("0.01"),
            direction="buy",
            max_orders=1,
            wait_time=1,
            exchange="lighter",
            grid_step=Decimal("0"),
            stop_price=Decimal("0"),
            pause_price=Decimal("0"),
            boost_mode=False,
            maker_depth_level=int(self._last_hot_update.get("aster_maker_depth_level", 10) or 10),
        )
        # LighterClient consumes these optional profile attributes before it
        # falls back to process environment; keep this strategy RH-only even
        # when an operator starts it from a shell with stale Core variables.
        setattr(trading_config, "lighter_environment", "robinhood")

        self._lighter_config = trading_config
        lighter_client = cast(
            LighterClient,
            ExchangeFactory.create_exchange("lighter", trading_config),  # type: ignore[arg-type]
        )
        lighter_client.setup_order_update_handler(self._handle_lighter_order_update)
        self._lighter_client = lighter_client
        self._validate_robinhood_credentials(lighter_client)
        self._initialize_runtime_state(lighter_client.account_index)
        self._acquire_instance_lock()
        self._load_ownership_state()
        await self._lighter_client.connect()
        await self._ensure_lighter_account_tier()
        contract_id, tick_size = await self._lighter_client.get_contract_attributes()
        trading_config.contract_id = contract_id
        trading_config.tick_size = tick_size
        runtime_step = self._runtime_quantity_step()
        if runtime_step is not None and runtime_step > 0:
            self._quantity_step = runtime_step
        await self._reconcile_startup_orders()
        await self._configure_lighter_leverage()
        await self._lighter_client.wait_for_market_data(timeout=10)

        # Reset session aggregates for a fresh run
        self._lighter_inventory_base = Decimal("0")
        self._lighter_avg_entry_price = Decimal("0")
        self._lighter_session_realized_pnl = Decimal("0")
        self._lighter_session_volume_quote = Decimal("0")
        self._lighter_session_volume_base = Decimal("0")
        self._lighter_order_fills.clear()
        self._binance_session_realized_pnl = Decimal("0")
        self._binance_inventory_base = initial_binance_position
        self._binance_avg_entry_price = initial_binance_avg_price
        self._binance_last_mark_price = initial_binance_mark
        self._binance_session_volume_quote = Decimal("0")
        self._binance_session_volume_base = Decimal("0")

        self._running = True
        await self._shutdown_state_task()
        await self._update_state_guarded(force=True)
        self._state_task = asyncio.create_task(self._state_maintainer())

        self.logger.log(
            f"Initialized simple market maker: contract={contract_id}, tick_size={tick_size}, "
            f"order_qty={self.settings.order_quantity}",
            "INFO",
        )

    def _initialize_runtime_state(self, account_index: int) -> None:
        configured = self.settings.ownership_state_path
        if configured:
            state_path = Path(configured).expanduser()
            if not state_path.is_absolute():
                state_path = PROJECT_ROOT / state_path
        else:
            ticker = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.settings.lighter_ticker.upper())
            state_path = PROJECT_ROOT / "logs" / f"rh_lighter_maker_{account_index}_{ticker}.json"
        self._ownership_state_path = state_path.resolve()
        self._instance_lock_path = self._ownership_state_path.with_suffix(
            self._ownership_state_path.suffix + ".lock"
        )

    @staticmethod
    def _process_exists(pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            # ``os.kill(pid, 0)`` is not a non-destructive probe on Windows;
            # use a query-only process handle instead.
            try:
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1000, False, pid)  # QUERY_LIMITED_INFORMATION
                if handle:
                    kernel32.CloseHandle(handle)
                    return True
                return ctypes.get_last_error() == 5  # access denied => process exists
            except Exception:
                return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def _acquire_instance_lock(self) -> None:
        path = self._instance_lock_path
        if path is None:
            raise RuntimeError("Maker runtime state path was not initialized")
        path.parent.mkdir(parents=True, exist_ok=True)
        token = secrets.token_hex(16)
        payload = json.dumps({"pid": os.getpid(), "token": token})

        for _ in range(2):
            try:
                fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                try:
                    existing = json.loads(path.read_text(encoding="utf-8"))
                    existing_pid = int(existing.get("pid", 0))
                except Exception:
                    existing_pid = 0
                if existing_pid and self._process_exists(existing_pid):
                    raise RuntimeError(
                        f"Another Robinhood maker process is already running for this account "
                        f"(pid={existing_pid}, lock='{path}')"
                    )
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                continue
            os.write(fd, payload.encode("utf-8"))
            self._instance_lock_fd = fd
            self._instance_lock_token = token
            return

        raise RuntimeError(f"Unable to acquire maker instance lock '{path}'")

    def _release_instance_lock(self) -> None:
        fd = self._instance_lock_fd
        self._instance_lock_fd = None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

        path = self._instance_lock_path
        if path is None or not self._instance_lock_token:
            return
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing.get("token") == self._instance_lock_token:
                path.unlink()
        except FileNotFoundError:
            pass
        except Exception as exc:
            self.logger.log(f"Failed to release maker instance lock '{path}': {exc}", "ERROR")
        finally:
            self._instance_lock_token = ""

    def _load_ownership_state(self) -> None:
        path = self._ownership_state_path
        if path is None or not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            values = payload.get("client_order_indices", [])
            if not isinstance(values, list):
                raise ValueError("client_order_indices must be a list")
            loaded: set[str] = set()
            for value in values:
                normalized = str(int(value))
                if not 0 <= int(normalized) < (1 << 48):
                    raise ValueError(f"client order index is outside uint48: {value!r}")
                loaded.add(normalized)
        except Exception as exc:
            raise RuntimeError(f"Invalid maker ownership state '{path}': {exc}") from exc
        self._own_client_order_indices.update(loaded)

    def _persist_ownership_state(self) -> None:
        path = self._ownership_state_path
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "account_index": getattr(self._lighter_client, "account_index", None),
            "ticker": self.settings.lighter_ticker.upper(),
            "client_order_indices": sorted(
                self._own_client_order_indices,
                key=lambda value: int(value),
            ),
            "updated_at_ms": int(time.time() * 1000),
        }
        temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        try:
            temp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            os.replace(temp_path, path)
            if os.name != "nt":
                os.chmod(path, 0o600)
        except Exception:
            with contextlib.suppress(OSError):
                temp_path.unlink()
            raise

    def _remember_owned_client_index(self, value: Any) -> str:
        normalized = str(int(value))
        if not 0 <= int(normalized) < (1 << 48):
            raise ValueError(f"Lighter client_order_index is outside uint48: {value!r}")
        self._own_client_order_indices.add(normalized)
        self._persist_ownership_state()
        return normalized

    def _discard_owned_client_index(self, value: Any) -> None:
        if value is None:
            return
        try:
            normalized = str(int(value))
        except (TypeError, ValueError):
            normalized = str(value).strip()
        if normalized and normalized in self._own_client_order_indices:
            self._own_client_order_indices.discard(normalized)
            self._persist_ownership_state()

    async def _reconcile_startup_orders(self) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None

        saved_indices = set(self._own_client_order_indices)
        active_orders: list[Any] = []
        deadline = time.monotonic() + max(
            0.5,
            float(self.settings.order_ack_timeout_seconds),
        )
        last_error: Optional[BaseException] = None
        while True:
            try:
                active_orders = await self._lighter_client.get_active_orders(
                    self._lighter_config.contract_id
                )
                last_error = None
            except Exception as exc:
                last_error = exc

            if last_error is None:
                unmanaged = [order for order in active_orders if not self._is_own_order(order)]
                if unmanaged:
                    order_ids = [str(getattr(order, "order_id", "unknown")) for order in unmanaged]
                    raise RuntimeError(
                        "Robinhood maker requires an exclusive clean market account; "
                        f"found unmanaged active order(s): {order_ids}. Cancel them or use a dedicated account."
                    )

                active_owned_indices = {
                    client_index
                    for client_index in (self._order_client_index(order) for order in active_orders)
                    if client_index is not None
                }
                if not saved_indices or saved_indices & active_owned_indices:
                    break

            if time.monotonic() >= deadline:
                if last_error is not None:
                    raise RuntimeError(
                        f"Could not reconcile persisted maker orders during startup: {last_error}"
                    ) from last_error
                raise RuntimeError(
                    "Persisted maker order IDs were not visible after the reconciliation window; "
                    "manual order-status review is required before restarting"
                )
            await asyncio.sleep(0.25)

        active_owned_indices = {
            client_index
            for client_index in (self._order_client_index(order) for order in active_orders)
            if client_index is not None
        }
        if saved_indices - active_owned_indices:
            # A persisted ID can be absent only after the exchange has
            # answered consistently for the full window. Keep it in state and
            # stop instead of forgetting an order whose REST visibility lagged.
            raise RuntimeError(
                "Persisted maker order reconciliation is incomplete; refusing to quote"
            )

        if not active_orders:
            self._tracked_orders.clear()
            self._persist_ownership_state()
            return

        for order in active_orders:
            client_index = self._order_client_index(order)
            if client_index is None:
                continue
            side = str(getattr(order, "side", "")).lower()
            if side not in {"buy", "sell"}:
                continue
            self._tracked_orders.setdefault(
                side,
                ActiveOrder(
                    order_id=str(order.order_id),
                    price=Decimal(str(order.price)),
                    side=side,
                    client_order_index=client_index,
                    created_at=0.0,
                    confirmed=True,
                ),
            )

        self.logger.log(
            f"Found {len(active_orders)} persisted maker quote(s); cancelling before restart",
            "WARNING",
        )
        await self._cancel_all_orders(reconciliation_attempts=12)

    @staticmethod
    def _validate_robinhood_credentials(client: LighterClient) -> None:
        """Reject reserved key indexes and placeholder/malformed RH keys."""

        account_index = getattr(client, "account_index", None)
        if not isinstance(account_index, int) or account_index < 0:
            raise ValueError(f"Invalid Robinhood Lighter account index: {account_index!r}")

        key_map = getattr(client, "api_private_keys", None)
        if not isinstance(key_map, dict) or not key_map:
            raise ValueError("Robinhood Lighter API credentials are missing")
        invalid_indexes = sorted(
            index
            for index in key_map
            if not isinstance(index, int) or index < 4 or index > 254
        )
        if invalid_indexes:
            raise ValueError(
                "Robinhood Lighter API key indexes must be in the conservative range 4..254; "
                f"invalid indexes: {invalid_indexes}"
            )
        # The current Lighter SDK generates a 40-byte API key (80 hex
        # characters, optionally prefixed with 0x).  Older SDKs emitted a
        # 32-byte value, so keep accepting that format for existing accounts.
        valid_key_pattern = r"(?:0x)?(?:[0-9a-fA-F]{80}|[0-9a-fA-F]{64})"
        invalid_key_indexes = [
            index
            for index, private_key in key_map.items()
            if re.fullmatch(valid_key_pattern, str(private_key).strip()) is None
        ]
        if invalid_key_indexes:
            raise ValueError(
                "Robinhood Lighter API private keys must use an optional 0x followed by 80 hexadecimal characters "
                "(64 hexadecimal characters are accepted for older SDK keys); "
                f"invalid key indexes: {sorted(invalid_key_indexes)}"
            )

    async def _ensure_lighter_account_tier(self) -> None:
        if self._lighter_client is None:
            return

        # Account-tier changes require a deliberate operator action and, on
        # Robinhood, can require an empty account plus a cooldown. Never turn a
        # small maker deployment into an account-management transaction.
        if (self.settings.lighter_environment or "").strip().lower() == "robinhood":
            self.logger.log("Skipping automatic Robinhood account-tier changes", "INFO")
            return

        target_tier = os.getenv("LIGHTER_TARGET_ACCOUNT_TIER", "premium") or "premium"
        target_tier = target_tier.strip()
        if not target_tier:
            self.logger.log(
                "LIGHTER_TARGET_ACCOUNT_TIER is empty; skipping automatic tier update",
                "INFO",
            )
            return

        target_tier_id_env = os.getenv("LIGHTER_TARGET_ACCOUNT_TIER_ID")
        target_tier_id: Optional[int] = None
        if target_tier_id_env:
            try:
                target_tier_id = int(target_tier_id_env)
            except ValueError:
                self.logger.log(
                    f"Invalid LIGHTER_TARGET_ACCOUNT_TIER_ID '{target_tier_id_env}'; ignoring value",
                    "WARNING",
                )

        ensure_method = getattr(self._lighter_client, "ensure_account_tier", None)
        if not callable(ensure_method):  # pragma: no cover - unexpected client regression
            self.logger.log(
                "Current Lighter client does not support tier management; skipping auto upgrade",
                "WARNING",
            )
            return

        ensure_callable = cast(Callable[..., Awaitable[bool]], ensure_method)

        self.logger.log(
            f"Ensuring Lighter account tier target='{target_tier}'"
            + (f" (expected id {target_tier_id})" if target_tier_id is not None else ""),
            "INFO",
        )

        try:
            success = await ensure_callable(target_tier=target_tier, target_tier_id=target_tier_id)
        except Exception as exc:  # pragma: no cover - network/SDK failures
            self.logger.log(
                f"Failed to enforce Lighter account tier '{target_tier}': {exc}",
                "ERROR",
            )
            return

        if success:
            self.logger.log(
                f"Lighter account tier enforcement completed for target '{target_tier}'",
                "INFO",
            )
        else:
            self.logger.log(
                "Lighter account tier enforcement reported a potential issue; review preceding warnings",
                "WARNING",
            )

    async def _configure_lighter_leverage(self) -> None:
        if self._lighter_client is None:
            return

        leverage_limits: Dict[str, Optional[int]] = {}
        limits_getter = getattr(self._lighter_client, "get_leverage_limits", None)
        if callable(limits_getter):
            try:
                leverage_limits = cast(Dict[str, Optional[int]], limits_getter())
            except Exception as exc:  # pragma: no cover - defensive logging
                if self.settings.lighter_leverage is not None:
                    raise RuntimeError(f"Failed to load Lighter leverage limits: {exc}") from exc
                self.logger.log(f"Failed to load Lighter leverage limits: {exc}", "WARNING")
                return
        else:  # pragma: no cover - unexpected SDK change
            if self.settings.lighter_leverage is not None:
                raise RuntimeError("Current Lighter client does not expose leverage metadata")
            self.logger.log(
                "Current Lighter client does not expose leverage metadata; skipping auto configuration",
                "WARNING",
            )
            return

        max_leverage = leverage_limits.get("max")
        default_leverage = leverage_limits.get("default")
        if max_leverage in (None, 0):
            if self.settings.lighter_leverage is not None:
                raise RuntimeError(
                    "Unable to determine Lighter max leverage from live market metadata"
                )
            self.logger.log(
                "Unable to determine Lighter max leverage from market metadata; set leverage manually if needed",
                "WARNING",
            )
            return

        if self.settings.lighter_leverage is None:
            target_leverage = int(max_leverage)
        else:
            try:
                target_leverage = int(self.settings.lighter_leverage)
            except (TypeError, ValueError):
                self.logger.log(
                    f"Invalid configured Lighter leverage: {self.settings.lighter_leverage}",
                    "ERROR",
                )
                return

        if target_leverage <= 0:
            self.logger.log(f"Ignoring non-positive Lighter leverage limit: {target_leverage}", "WARNING")
            return

        default_display = str(default_leverage) if default_leverage is not None else "unknown"
        if target_leverage > int(max_leverage):
            raise ValueError(
                f"Configured Lighter leverage {target_leverage}x exceeds market maximum {max_leverage}x"
            )
        if self.settings.lighter_leverage is None:
            self.logger.log(
                f"Targeting Lighter max leverage {target_leverage}x (default {default_display}x)",
                "INFO",
            )
        else:
            self.logger.log(
                f"Targeting Lighter leverage {target_leverage}x (market max {max_leverage}x, default {default_display}x)",
                "INFO",
            )

        await self._ensure_lighter_leverage(target_leverage)

    async def _ensure_lighter_leverage(self, leverage: int) -> None:
        if leverage <= 0:
            return
        if self._lighter_client is None or self._lighter_config is None:
            raise RuntimeError("Cannot update Lighter leverage: client not initialized")

        signer_client = getattr(self._lighter_client, "lighter_client", None)
        if signer_client is None:
            raise RuntimeError("Cannot update Lighter leverage: signer client unavailable")

        contract_id = getattr(self._lighter_config, "contract_id", None)
        if contract_id is None:
            raise RuntimeError("Cannot update Lighter leverage: contract id not resolved")
        try:
            market_index = int(contract_id)
        except (TypeError, ValueError):
            raise RuntimeError(
                f"Cannot update Lighter leverage: invalid contract id '{contract_id}'"
            )

        margin_mode = getattr(signer_client, "CROSS_MARGIN_MODE", None)
        if margin_mode is None:
            raise RuntimeError("Cannot update Lighter leverage: margin mode unavailable")

        try:
            tx_info, _, err = await signer_client.update_leverage(
                market_index,
                margin_mode,
                int(leverage),
            )
        except Exception as exc:  # pragma: no cover - network/SDK failures
            raise RuntimeError(
                f"Failed to update Lighter leverage to {leverage}x: {exc}"
            ) from exc

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

    async def _shutdown_state_task(self) -> None:
        if self._state_task is None:
            return

        task = self._state_task
        self._state_task = None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def stop(self) -> None:
        if self._stop_completed:
            if self._stop_error is not None:
                raise self._stop_error
            return

        self._running = False
        await self._shutdown_state_task()
        shutdown_errors: list[BaseException] = []

        if (
            self._lighter_client is not None
            and self._lighter_config is not None
            and getattr(self._lighter_config, "contract_id", "")
        ):
            try:
                shutdown_timeout = max(0.1, float(self.settings.shutdown_timeout_seconds))
                await asyncio.wait_for(
                    self._cancel_all_orders(reconciliation_attempts=3),
                    timeout=shutdown_timeout,
                )
            except asyncio.TimeoutError as exc:
                shutdown_errors.append(
                    RuntimeError(
                        "Timed out confirming quote cancellation during shutdown; "
                        "the next startup will reconcile persisted order IDs"
                    )
                )
                self.logger.log(
                    "Timed out confirming quote cancellation during shutdown; "
                    "continuing to close connections and release the instance lock",
                    "ERROR",
                )
            except Exception as exc:
                shutdown_errors.append(exc)

        if self._lighter_client is not None:
            try:
                disconnect_timeout = max(
                    0.1,
                    min(float(self.settings.shutdown_timeout_seconds), 5.0),
                )
                await asyncio.wait_for(
                    self._lighter_client.disconnect(),
                    timeout=disconnect_timeout,
                )
            except asyncio.TimeoutError:
                shutdown_errors.append(RuntimeError("Timed out closing the Lighter connection"))
            except Exception as exc:
                shutdown_errors.append(exc)

        try:
            if self._session and not self._session.closed:
                await self._session.close()
        except Exception as exc:
            shutdown_errors.append(exc)

        self._release_instance_lock()

        self._stop_completed = True
        if shutdown_errors:
            detail = "; ".join(str(error) for error in shutdown_errors)
            self._stop_error = RuntimeError(f"Market-maker shutdown was not clean: {detail}")
            raise self._stop_error from shutdown_errors[0]

    async def run(self) -> None:
        if not self._running:
            raise RuntimeError("SimpleMarketMaker.start() must be called first")

        completed_iterations = 0
        try:
            while self._running:
                try:
                    await self._iteration()
                except Exception as exc:  # noqa: BLE001
                    delay = self._handle_iteration_failure(exc)
                    if delay is None:
                        raise
                    try:
                        await self._cancel_all_orders(reconciliation_attempts=3)
                    except Exception as cancel_exc:
                        self._external_pause = True
                        self.logger.log(
                            "Could not confirm quote withdrawal during transient failure: "
                            f"{cancel_exc}",
                            "ERROR",
                        )
                        raise RuntimeError(
                            "Stopping maker because quote withdrawal could not be confirmed"
                        ) from cancel_exc
                    await asyncio.sleep(delay)
                    continue

                self._reset_rate_limit_backoff()
                completed_iterations += 1
                if self.settings.max_cycles > 0 and completed_iterations >= self.settings.max_cycles:
                    self.logger.log(
                        f"Completed requested maker iterations: {completed_iterations}",
                        "INFO",
                    )
                    break
                await asyncio.sleep(self.settings.loop_sleep_seconds)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            self.logger.log("Maker loop cancelled", "WARNING")
        finally:
            await self.stop()

    async def _iteration(self) -> None:
        async with self._quote_operation_lock:
            await self._refresh_quotes()

    async def _resolve_binance_imbalance(self) -> Decimal:
        """Read a short-lived Binance pressure signal, or return neutral."""

        if not self.settings.use_binance_reference or self._binance_reference is None:
            return Decimal("0")

        try:
            imbalance = await asyncio.wait_for(
                self._binance_reference.fetch_orderbook_imbalance(
                    self.settings.binance_depth_levels,
                ),
                timeout=max(0.1, self.settings.binance_reference_timeout_seconds),
            )
            if imbalance.is_finite() and Decimal("-1") <= imbalance <= Decimal("1"):
                return imbalance
        except Exception as exc:  # pragma: no cover - network dependent
            self.logger.log(f"Binance orderbook signal unavailable: {exc}", "WARNING")

        # A stale directional signal is more dangerous than a neutral signal.
        # Continue using the local Lighter midpoint if Binance is unavailable.
        return Decimal("0")

    async def _resolve_reference_mid(self, fallback_mid: Decimal) -> Decimal:
        """Apply Binance orderbook pressure to the local Lighter midpoint.

        The method name remains for compatibility with older integrations, but
        Binance's absolute midpoint is intentionally never used here.
        """

        imbalance = await self._resolve_binance_imbalance()
        return apply_orderbook_imbalance(
            fallback_mid,
            imbalance,
            self.settings.binance_imbalance_max_bps,
        )

    async def _refresh_quotes(self) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None

        hot_update = await self._load_hot_update()
        if not hot_update.get("cycle_enabled", True):
            self.logger.log("Cycle paused via hot update; sleeping", "WARNING")
            await self._cancel_all_orders()
            return

        if self._external_pause:
            if not self._pause_enforced:
                self.logger.log("External pause active; cancelling outstanding orders", "WARNING")
                await self._cancel_all_orders()
                self._pause_enforced = True
            return

        if self._pause_enforced:
            self._pause_enforced = False

        contract_id = self._lighter_config.contract_id
        best_bid, best_ask = await self._lighter_client.fetch_bbo_prices(contract_id)

        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log(
                "Invalid Lighter depth snapshot; cancelling own quotes until the book recovers",
                "WARNING",
            )
            await self._cancel_all_orders()
            return

        now = time.time()
        max_age = self._state_refresh_interval * 2.0
        if (now - self._latest_net_position_time) > max_age:
            await self._refresh_state_if_needed(max_age=max_age)

        net_position = self._latest_net_position
        if not self._inventory_state_known:
            self.logger.log(
                "Inventory state is unknown; cancelling own quotes and waiting for reconciliation",
                "WARNING",
            )
            await self._cancel_all_orders()
            return
        if self.settings.enable_binance_hedge and not self._binance_state_known:
            self.logger.log(
                "Binance hedge position is unknown; cancelling own quotes until reconciliation",
                "WARNING",
            )
            await self._cancel_all_orders()
            return

        lighter_mid = (best_bid + best_ask) / 2
        # Lighter supplies the absolute price scale. Binance contributes only
        # a bounded, dimensionless orderbook-pressure shift, so differing
        # quote assets or contract multipliers cannot create a cross-market
        # absolute-price jump.
        mid_price = await self._resolve_reference_mid(lighter_mid)
        self._lighter_last_mark_price = lighter_mid
        spread_scale = self._resolve_spread_scale(hot_update)
        inventory_cap = self.settings.effective_inventory_limit()
        mid_price = apply_inventory_skew(
            mid_price,
            net_position,
            inventory_cap,
            self.settings.inventory_skew_bps,
        )
        targets = compute_target_prices(mid_price, spread_scale, self._lighter_config.tick_size)
        targets = clamp_maker_targets(
            targets,
            best_bid,
            best_ask,
            self._lighter_config.tick_size,
            max_bbo_distance_ticks=self.settings.bbo_max_distance_ticks,
        )

        max_quote_quantity = self._max_quote_quantity()
        bid_quote_quantity = self._normalize_order_quantity(
            max_quote_quantity,
            targets["buy"],
        )
        ask_quote_quantity = self._normalize_order_quantity(
            max_quote_quantity,
            targets["sell"],
        )
        bid_enabled = side_has_inventory_capacity(
            net_position,
            inventory_cap,
            "buy",
            bid_quote_quantity,
        )
        ask_enabled = side_has_inventory_capacity(
            net_position,
            inventory_cap,
            "sell",
            ask_quote_quantity,
        )

        # One active-order snapshot per iteration avoids two authenticated REST
        # reads and reduces rate-limit pressure during quote refreshes.
        active_orders = await self._lighter_client.get_active_orders(contract_id)
        await self._sync_side("buy", targets["buy"], bid_enabled, active_orders=active_orders)
        await self._sync_side("sell", targets["sell"], ask_enabled, active_orders=active_orders)

        await self._maybe_execute_hedge(net_position)

    async def _sync_side(
        self,
        side: str,
        target_price: Decimal,
        enabled: bool,
        *,
        active_orders: Optional[Iterable[Any]] = None,
    ) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None

        if active_orders is None:
            active_orders = await self._lighter_client.get_active_orders(self._lighter_config.contract_id)
        side_orders = [order for order in active_orders if order.side == side]
        unmanaged_orders = [order for order in side_orders if not self._is_own_order(order, side)]
        if unmanaged_orders:
            self.logger.log(
                f"Leaving {len(unmanaged_orders)} unmanaged {side} order(s) untouched",
                "WARNING",
            )
        relevant_orders = [order for order in side_orders if self._is_own_order(order, side)]
        pending = self._tracked_orders.get(side)
        if not relevant_orders and pending is not None:
            pending_age = max(0.0, time.monotonic() - pending.created_at)
            if not pending.confirmed:
                if pending_age <= self.settings.order_ack_timeout_seconds:
                    return
                raise RuntimeError(
                    f"Lighter {side} quote {pending.client_order_index or pending.order_id} "
                    f"was not confirmed within {self.settings.order_ack_timeout_seconds:.1f}s"
                )
            # A confirmed order disappearing from one active-order snapshot is
            # not proof that it filled or cancelled. Keep the side blocked and
            # fail closed after a second miss rather than stacking a replacement
            # on top of a quote that may still be resting.
            pending.missing_active_snapshots += 1
            if pending.missing_active_snapshots <= 2:
                return
            raise RuntimeError(
                f"Confirmed Lighter {side} quote {pending.client_order_index or pending.order_id} "
                "disappeared from active-order reconciliation without a terminal update"
            )
        if unmanaged_orders and not relevant_orders:
            # Do not submit another quote beside an order this process cannot
            # prove it owns (for example, after a restart).
            self._tracked_orders.pop(side, None)
            return
        replace_threshold = max(
            self._lighter_config.tick_size * Decimal(self.settings.order_refresh_ticks),
            abs(target_price) * self.settings.order_refresh_bps / Decimal("10000"),
        )

        if side not in self._allowed_sides:
            for order in relevant_orders:
                try:
                    cancel_result = await self._lighter_client.cancel_order(order.order_id)
                    if getattr(cancel_result, "success", False):
                        self.logger.log(
                            f"Requested cancellation of {side} order {order.order_id} due to side whitelist",
                            "INFO",
                        )
                    else:
                        self.logger.log(
                            f"Failed to request cancellation of {side} order {order.order_id}: "
                            f"{getattr(cancel_result, 'error_message', 'unknown error')}",
                            "ERROR",
                        )
                except Exception as exc:  # pragma: no cover - defensive
                    self.logger.log(f"Failed to cancel order {order.order_id}: {exc}", "ERROR")
            if side in self._tracked_orders:
                del self._tracked_orders[side]
            return

        kept: Iterable[ActiveOrder] = ()
        cancellation_attempted = False
        for idx, order in enumerate(relevant_orders):
            price_diff = abs(order.price - target_price)
            previous = self._tracked_orders.get(side)
            client_index = self._order_client_index(order)
            same_order = previous is not None and (
                previous.order_id == str(order.order_id)
                or (
                    client_index is not None
                    and previous.client_order_index == client_index
                )
            )
            created_at = previous.created_at if same_order else time.monotonic()
            quote_age = max(0.0, time.monotonic() - created_at)
            in_minimum_lifetime = quote_age < self.settings.min_quote_lifetime_seconds
            emergency_threshold = max(
                replace_threshold,
                abs(target_price) * Decimal("2") / Decimal("10000"),
            )
            keep_for_lifetime = in_minimum_lifetime and price_diff < emergency_threshold
            keep = enabled and idx == 0 and (
                price_diff <= replace_threshold or keep_for_lifetime
            )
            if keep:
                self._tracked_orders[side] = ActiveOrder(
                    order_id=order.order_id,
                    price=order.price,
                    side=side,
                    client_order_index=client_index,
                    created_at=created_at,
                    confirmed=True,
                    missing_active_snapshots=0,
                )
                kept = (self._tracked_orders[side],)
                continue
            try:
                cancel_result = await self._lighter_client.cancel_order(order.order_id)
                cancellation_attempted = True
                if getattr(cancel_result, "success", False):
                    self.logger.log(
                        f"Requested cancellation of stale {side} order {order.order_id}",
                        "INFO",
                    )
                else:
                    self.logger.log(
                        f"Failed to request cancellation of {side} order {order.order_id}: "
                        f"{getattr(cancel_result, 'error_message', 'unknown error')}",
                        "ERROR",
                    )
            except Exception as exc:
                cancellation_attempted = True
                self.logger.log(f"Failed to cancel order {order.order_id}: {exc}", "ERROR")

        if not enabled:
            if side in self._tracked_orders:
                del self._tracked_orders[side]
            return

        if kept:
            return

        # A send acknowledgement is not a sequencer cancellation confirmation.
        # Never stack a replacement beside an order cancelled in this same
        # iteration; the next active-order snapshot must prove it disappeared.
        if cancellation_attempted:
            return

        if self._fill_cooldown_seconds > 0:
            last_fill = self._last_fill_timestamp.get(side, 0.0)
            cooldown_remaining = (last_fill + self._fill_cooldown_seconds) - time.time()
            if cooldown_remaining > 0:
                if side in self._tracked_orders:
                    self._tracked_orders.pop(side, None)
                self.logger.log(
                    (
                        "Skipping {side} order placement for {remaining:.2f}s due to recent fill"
                    ).format(side=side, remaining=cooldown_remaining),
                    "INFO",
                )
                return

        # PAUSE/FLATTEN can arrive while this iteration is awaiting market or
        # account data. Recheck immediately before the only state-changing
        # operation so a command cannot be followed by a late replacement.
        if self._external_pause or self._flatten_active:
            return

        order_quantity = self._normalize_order_quantity(
            self._resolve_order_quantity(),
            target_price,
        )
        if order_quantity <= 0:
            self.logger.log(
                f"Skipping {side} quote: quantity is below the runtime market minimum",
                "WARNING",
            )
            return

        place_kwargs: Dict[str, Any] = {"time_in_force": "post_only"}
        reserved_client_index: Optional[str] = None
        reserve_client_index = getattr(
            self._lighter_client,
            "reserve_client_order_index",
            None,
        )
        if callable(reserve_client_index):
            reserved_client_index = self._remember_owned_client_index(
                reserve_client_index()
            )
            place_kwargs["client_order_index"] = int(reserved_client_index)
            # Persist ownership and install the pending guard before the
            # network call. A kill -9 after send therefore remains recoverable
            # on the next startup.
            self._tracked_orders[side] = ActiveOrder(
                order_id=reserved_client_index,
                price=target_price,
                side=side,
                client_order_index=reserved_client_index,
                created_at=time.monotonic(),
                confirmed=False,
                missing_active_snapshots=0,
            )

        order_result = await self._lighter_client.place_limit_order(
            self._lighter_config.contract_id,
            order_quantity,
            target_price,
            side,
            **place_kwargs,
        )
        if not order_result.success:
            self.logger.log(f"Failed to place {side} order: {order_result.error_message}", "ERROR")
            return
        if order_result.order_id:
            if (
                reserved_client_index is not None
                and reserved_client_index not in self._own_client_order_indices
            ):
                self.logger.log(
                    f"Lighter {side} quote reached a terminal state before send acknowledgement",
                    "WARNING",
                )
                return
            returned_client_index = self._remember_owned_client_index(order_result.order_id)
            if (
                reserved_client_index is not None
                and returned_client_index != reserved_client_index
            ):
                self._discard_owned_client_index(reserved_client_index)
            existing = self._tracked_orders.get(side)
            if (
                existing is None
                or existing.client_order_index != returned_client_index
            ):
                self._tracked_orders[side] = ActiveOrder(
                    order_id=returned_client_index,
                    price=target_price,
                    side=side,
                    client_order_index=returned_client_index,
                    created_at=time.monotonic(),
                    confirmed=False,
                    missing_active_snapshots=0,
                )
        price_display = self._format_decimal(target_price, 6)
        qty_display = self._format_decimal(order_quantity, 6)
        self.logger.log(
            (
                "Submitted {side} order request client_id={client_id} qty={qty} @ {price}"
            ).format(side=side, client_id=order_result.order_id, price=price_display, qty=qty_display),
            "INFO",
        )

    async def _maybe_execute_hedge(self, net_position: Decimal) -> None:
        if self._hedger is None:
            return

        now = time.monotonic()
        if now - self._last_hedge_timestamp < self.settings.hedge_cooldown_seconds:
            return

        combined_position = net_position + self._binance_position_estimate
        hedge_qty = required_hedge_quantity(
            combined_position,
            self.settings.hedge_threshold,
            self.settings.hedge_buffer,
        )
        if hedge_qty <= 0:
            return

        max_hedge_quantity = self.settings.max_hedge_quantity
        if max_hedge_quantity is not None and max_hedge_quantity > 0:
            hedge_qty = min(hedge_qty, max_hedge_quantity)

        hedge_side = "SELL" if combined_position > 0 else "BUY"
        raw_hedge_qty = hedge_qty
        # Throttle attempts as well as successful fills. Repeatedly retrying an
        # uncertain cross-venue response is riskier than waiting to reconcile.
        self._last_hedge_timestamp = now
        try:
            hedge_qty = await self._hedger.prepare_market_quantity(hedge_qty)
        except Exception as exc:
            self.logger.log(f"Failed to normalize Binance hedge quantity: {exc}", "ERROR")
            return

        if hedge_qty <= 0:
            constraints = self._hedger.lot_size_constraints()
            step_size = constraints.get("step_size") if isinstance(constraints, dict) else None
            min_qty = constraints.get("min_quantity") if isinstance(constraints, dict) else None
            step_str = self._format_decimal(step_size, 6) if isinstance(step_size, Decimal) else "n/a"
            min_qty_str = self._format_decimal(min_qty, 6) if isinstance(min_qty, Decimal) else "n/a"
            self.logger.log(
                (
                    "Skipped Binance hedge: rawQty={raw} normalized below minimum lot "
                    "(step={step}, minQty={min_qty})"
                ).format(
                    raw=self._format_decimal(raw_hedge_qty, 6),
                    step=step_str,
                    min_qty=min_qty_str,
                ),
                "INFO",
            )
            return

        try:
            order_response = await self._hedger.place_market_order(hedge_side, hedge_qty)
            executed_raw = (
                order_response.get("executedQty")
                or order_response.get("cumQty")
            )
            if executed_raw is None:
                self._binance_state_known = False
                self.logger.log(
                    "Binance hedge response omitted executed quantity; waiting for account reconciliation",
                    "ERROR",
                )
                return
            executed_qty = abs(self._to_decimal(executed_raw))
            if executed_qty <= 0:
                self._binance_state_known = False
                self.logger.log(
                    "Binance hedge response reported zero execution; waiting for account reconciliation",
                    "ERROR",
                )
                return

            signed_qty = executed_qty if hedge_side == "BUY" else -executed_qty
            fill_price = self._resolve_binance_fill_price(
                order_response,
                executed_qty,
                self._lighter_last_mark_price,
            )
            self._binance_position_estimate += signed_qty
            self._binance_state_known = True
            if fill_price > 0:
                self._binance_last_mark_price = fill_price
            self._apply_binance_fill_to_session_pnl(signed_qty, fill_price)

            abs_qty = abs(executed_qty)
            self._binance_session_volume_base += abs_qty
            if fill_price > 0:
                self._binance_session_volume_quote += abs_qty * fill_price

            self.logger.log(
                (
                    "Executed Binance hedge: side={side}, qty={qty} (raw={raw}, lighter_pos={lighter}, binance_pos={binance}, "
                    "combined={combined})"
                ).format(
                    side=hedge_side,
                    qty=self._format_decimal(executed_qty, 6),
                    raw=self._format_decimal(raw_hedge_qty, 6),
                    lighter=self._format_decimal(net_position, 6),
                    binance=self._format_decimal(self._binance_position_estimate, 6),
                    combined=self._format_decimal(combined_position, 6),
                ),
                "INFO",
            )
        except Exception as exc:
            self._binance_state_known = False
            self.logger.log(f"Binance hedge failed: {exc}", "ERROR")

    async def _update_state_once(self, force: bool = False) -> None:
        if self._lighter_client is None:
            return

        metrics: Dict[str, Decimal]
        try:
            metrics = await self._lighter_client.get_account_metrics()
        except Exception as exc:
            self.logger.log(f"Failed to fetch Lighter account metrics: {exc}", "ERROR")
            try:
                position_size = await self._lighter_client.get_account_positions()
            except Exception as pos_exc:
                self.logger.log(f"Failed to fetch Lighter account position fallback: {pos_exc}", "ERROR")
                self._inventory_state_known = False
                self._latest_net_position_time = 0.0
                return
            metrics = {
                "position_size": position_size,
                "available_balance": Decimal("0"),
                "collateral": Decimal("0"),
                "total_asset_value": Decimal("0"),
                "daily_volume": Decimal("0"),
                "weekly_volume": Decimal("0"),
                "monthly_volume": Decimal("0"),
                "total_volume": Decimal("0"),
                "position_value": Decimal("0"),
                "unrealized_pnl": Decimal("0"),
                "realized_pnl": Decimal("0"),
            }

        self._inventory_state_known = True

        self._latest_metrics = metrics
        self._latest_net_position = metrics.get("position_size", self._latest_net_position)
        self._latest_net_position_time = time.time()

        position_size = metrics.get("position_size")
        if isinstance(position_size, Decimal):
            self._lighter_inventory_base = position_size
            if position_size == 0:
                self._lighter_avg_entry_price = Decimal("0")
            else:
                position_value = metrics.get("position_value", Decimal("0"))
                if isinstance(position_value, Decimal) and position_value != 0:
                    try:
                        derived_avg = abs(position_value) / abs(position_size)
                    except (InvalidOperation, ZeroDivisionError):
                        derived_avg = None
                    if derived_avg and derived_avg > 0:
                        self._lighter_avg_entry_price = derived_avg

        if force:
            self._last_metrics_time = 0.0

        await self._maybe_report_metrics(metrics, force=force)

    async def _update_state_guarded(self, *, force: bool = False) -> None:
        async with self._state_update_lock:
            await self._update_state_once(force=force)

    async def _refresh_state_if_needed(self, *, max_age: float) -> None:
        now = time.time()
        if now - self._latest_net_position_time <= max_age:
            return

        async with self._state_update_lock:
            now = time.time()
            if now - self._latest_net_position_time <= max_age:
                return
            await self._update_state_once()

    async def _state_maintainer(self) -> None:
        while self._running:
            try:
                await self._update_state_guarded()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.log(f"Background state update failed: {exc}", "ERROR")

            if not self._running:
                break

            await asyncio.sleep(self._state_refresh_interval)

    async def _cancel_all_orders(self, *, reconciliation_attempts: int = 3) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None
        if not self._own_client_order_indices and not self._tracked_orders:
            return
        attempts = max(1, int(reconciliation_attempts))
        observed_client_indices: set[str] = set()
        cancel_requested_order_ids: set[str] = set()
        last_query_error: Optional[BaseException] = None

        for attempt in range(attempts):
            try:
                active_orders = await self._lighter_client.get_active_orders(
                    self._lighter_config.contract_id
                )
            except Exception as exc:
                last_query_error = exc
                self.logger.log(
                    f"Failed to reconcile active orders during cancellation: {exc}",
                    "ERROR",
                )
                if attempt + 1 < attempts:
                    await asyncio.sleep(0.25)
                continue

            last_query_error = None
            own_orders = [
                order
                for order in active_orders
                if self._is_own_order(order, getattr(order, "side", None))
            ]
            active_client_indices = {
                client_index
                for client_index in (self._order_client_index(order) for order in own_orders)
                if client_index is not None
            }

            for client_index in observed_client_indices - active_client_indices:
                self._discard_owned_client_index(client_index)
                self._forget_tracked_order("", client_index)

            for order in own_orders:
                client_index = self._order_client_index(order)
                if client_index:
                    observed_client_indices.add(client_index)
                order_id = str(order.order_id)
                if order_id in cancel_requested_order_ids:
                    continue
                try:
                    result = await self._lighter_client.cancel_order(order.order_id)
                    if getattr(result, "success", False):
                        cancel_requested_order_ids.add(order_id)
                    else:
                        self.logger.log(
                            f"Cancellation request failed for order {order.order_id}: "
                            f"{getattr(result, 'error_message', 'unknown error')}",
                            "ERROR",
                        )
                except Exception as exc:
                    self.logger.log(f"Failed to cancel order {order.order_id}: {exc}", "ERROR")

            if not self._own_client_order_indices and not self._tracked_orders:
                return
            if attempt + 1 < attempts:
                await asyncio.sleep(0.25)

        unresolved = sorted(
            self._own_client_order_indices
            | {
                tracked.client_order_index or tracked.order_id
                for tracked in self._tracked_orders.values()
            }
        )
        if unresolved:
            detail = (
                f"; last reconciliation error: {last_query_error}"
                if last_query_error is not None
                else ""
            )
            raise RuntimeError(
                f"Could not confirm cancellation of {len(unresolved)} own quote(s): {unresolved}{detail}"
            )

    @staticmethod
    def _order_client_index(order: Any) -> Optional[str]:
        value = getattr(order, "client_order_index", None)
        if value is None and isinstance(order, dict):
            value = order.get("client_order_index") or order.get("clientOrderIndex")
        if value is None:
            return None
        try:
            text = str(int(value))
        except (TypeError, ValueError):
            text = str(value).strip()
        return text or None

    def _is_own_order(self, order: Any, side: Optional[str] = None) -> bool:
        """Return whether an active order belongs to this maker process."""

        client_index = self._order_client_index(order)
        if client_index is not None and client_index in self._own_client_order_indices:
            return True
        if side:
            tracked = self._tracked_orders.get(side)
            order_id = getattr(order, "order_id", None)
            if order_id is None and isinstance(order, dict):
                order_id = order.get("order_id") or order.get("id")
            if tracked is not None and str(order_id or "") == tracked.order_id:
                return True
        return False

    async def _load_hot_update(self) -> Dict[str, Any]:
        source = self.settings.config_path
        try:
            path = Path(source)
            if path.is_file():
                raw = await asyncio.to_thread(path.read_text, encoding="utf-8")
                data = json.loads(raw)
            else:
                assert self._session is not None
                async with self._session.get(source) as response:
                    data = await response.json()
            if isinstance(data, dict):
                self._last_hot_update = data
                return data
        except Exception as exc:
            self.logger.log(f"Failed to load hot update config '{source}': {exc}", "ERROR")
        if not isinstance(self._last_hot_update, dict):
            self._last_hot_update = {}
        if not self._last_hot_update:
            self._last_hot_update = {"cycle_enabled": True}
        return dict(self._last_hot_update)

    def _resolve_spread_scale(self, hot_update: Dict[str, Any]) -> Decimal:
        depth_level = hot_update.get("aster_maker_depth_level")
        try:
            depth_level = Decimal(str(depth_level))
        except Exception:
            depth_level = Decimal("10")
        base_level = Decimal("10")
        scale = depth_level / base_level if base_level > 0 else Decimal("1")
        spread = (self.settings.base_spread_bps * scale).quantize(Decimal("0.0001"))
        return spread

    @staticmethod
    def _format_decimal(value: Decimal, precision: int = 4) -> str:
        try:
            if precision <= 0:
                quant = Decimal("1")
            else:
                quant = Decimal("1") / (Decimal("10") ** precision)
            return f"{value.quantize(quant, rounding=ROUND_HALF_UP):f}"
        except Exception:
            return format(value, "f")

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    @staticmethod
    def _update_session_position(
        current_pos: Decimal,
        avg_price: Decimal,
        session_realized: Decimal,
        signed_quantity: Decimal,
        price: Decimal,
    ) -> tuple[Decimal, Decimal, Decimal]:
        if signed_quantity == 0 or price <= 0:
            return current_pos, avg_price, session_realized

        if current_pos == 0 or current_pos * signed_quantity > 0:
            new_pos = current_pos + signed_quantity
            if new_pos == 0:
                return Decimal("0"), Decimal("0"), session_realized

            if current_pos == 0:
                new_avg = price
            else:
                total_cost = (avg_price * current_pos) + (price * signed_quantity)
                new_avg = total_cost / new_pos if new_pos != 0 else Decimal("0")
            return new_pos, new_avg, session_realized

        closing_qty = min(abs(signed_quantity), abs(current_pos))
        if current_pos > 0:
            realized_delta = (price - avg_price) * closing_qty
        else:
            realized_delta = (avg_price - price) * closing_qty
        session_realized += realized_delta

        new_pos = current_pos + signed_quantity
        if new_pos == 0:
            new_avg = Decimal("0")
        elif new_pos * current_pos > 0:
            new_avg = avg_price
        else:
            new_avg = price

        return new_pos, new_avg, session_realized

    def _apply_fill_to_session_pnl(self, signed_quantity: Decimal, price: Decimal) -> None:
        new_pos, new_avg, new_realized = self._update_session_position(
            self._lighter_inventory_base,
            self._lighter_avg_entry_price,
            self._lighter_session_realized_pnl,
            signed_quantity,
            price,
        )
        self._lighter_inventory_base = new_pos
        self._lighter_avg_entry_price = new_avg
        self._lighter_session_realized_pnl = new_realized
        # Private fills arrive before the slower account REST refresh. Feed the
        # signed delta into the quote risk gate immediately; the next account
        # snapshot remains authoritative and will correct any discrepancy.
        self._latest_net_position = new_pos
        self._latest_net_position_time = time.time()
        self._inventory_state_known = True

    def _apply_binance_fill_to_session_pnl(self, signed_quantity: Decimal, price: Decimal) -> None:
        new_pos, new_avg, new_realized = self._update_session_position(
            self._binance_inventory_base,
            self._binance_avg_entry_price,
            self._binance_session_realized_pnl,
            signed_quantity,
            price,
        )
        self._binance_inventory_base = new_pos
        self._binance_avg_entry_price = new_avg
        self._binance_session_realized_pnl = new_realized

    @staticmethod
    def _compute_unrealized_pnl(position: Decimal, avg_price: Decimal, mark_price: Decimal) -> Decimal:
        if position == 0 or avg_price <= 0 or mark_price <= 0:
            return Decimal("0")
        return (mark_price - avg_price) * position

    def _resolve_binance_fill_price(
        self,
        order_response: Dict[str, Any],
        executed_qty: Decimal,
        fallback_price: Decimal,
    ) -> Decimal:
        if executed_qty > 0:
            cumulative_quote = order_response.get("cumQuote") or order_response.get("cummulativeQuoteQty")
            quote_value = self._to_decimal(cumulative_quote)
            if quote_value > 0:
                try:
                    price = quote_value / executed_qty
                    if price > 0:
                        return price
                except (InvalidOperation, ZeroDivisionError):
                    pass

        for key in (
            "avgPrice",
            "avg_price",
            "price",
            "stopPrice",
            "activatePrice",
            "executedPrice",
        ):
            price = self._to_decimal(order_response.get(key))
            if price > 0:
                return price

        fills = order_response.get("fills")
        if isinstance(fills, list):
            for fill in fills:
                if isinstance(fill, dict):
                    price = self._to_decimal(fill.get("price"))
                    if price > 0:
                        return price

        if fallback_price > 0:
            return fallback_price
        if self._lighter_last_mark_price > 0:
            return self._lighter_last_mark_price
        return Decimal("0")

    def _handle_lighter_order_update(self, update: Dict[str, Any]) -> None:
        if self._lighter_config is None:
            return

        try:
            contract_id = str(getattr(self._lighter_config, "contract_id", "") or "")
            update_contract_id = str(update.get("contract_id") or update.get("market_index") or "")
            if contract_id and update_contract_id and update_contract_id != contract_id:
                return

            status = str(update.get("status", "")).upper()
            terminal_status = (
                status == "FILLED"
                or status.startswith("CANCEL")
                or status.startswith("REJECT")
                or status.startswith("EXPIRED")
            )
            working_status = status in {
                "OPEN",
                "PENDING",
                "PENDING_NEW",
                "PARTIALLY_FILLED",
            }
            if not terminal_status and not working_status:
                return

            order_id = str(update.get("order_id") or "")
            if not order_id:
                return

            client_order_index_value = update.get("client_order_index") or update.get("clientOrderIndex")
            client_order_index = (
                str(client_order_index_value).strip()
                if client_order_index_value is not None
                else ""
            )
            if client_order_index:
                if client_order_index not in self._own_client_order_indices:
                    inflight_client_index = getattr(
                        self._lighter_client,
                        "current_order_client_id",
                        None,
                    )
                    if str(inflight_client_index or "") != client_order_index:
                        return
                    # The private WS acknowledgement can win the race against
                    # the REST send response. Adopt only the exact client id
                    # currently being submitted by this LighterClient.
                    client_order_index = self._remember_owned_client_index(
                        client_order_index
                    )
            elif not any(tracked.order_id == order_id for tracked in self._tracked_orders.values()):
                # Private account streams may omit the client id on older
                # payloads. Only accept an update if the resting order index is
                # already tracked by this process.
                return

            side = str(update.get("side") or "").lower()
            for tracked_side, tracked in tuple(self._tracked_orders.items()):
                if (
                    tracked.order_id == order_id
                    or (
                        client_order_index
                        and tracked.client_order_index == client_order_index
                    )
                ):
                    tracked.order_id = order_id
                    tracked.confirmed = working_status
                    if side in {"buy", "sell"} and tracked_side != side:
                        self._tracked_orders.pop(tracked_side, None)
                        tracked.side = side
                        self._tracked_orders[side] = tracked
                    break

            if working_status and status != "PARTIALLY_FILLED":
                return

            filled_total = abs(self._to_decimal(update.get("filled_size")))
            previous_filled = self._lighter_order_fills.get(order_id, Decimal("0"))
            delta_filled = filled_total - previous_filled
            if delta_filled <= 0:
                if terminal_status:
                    self._lighter_order_fills.pop(order_id, None)
                    if client_order_index:
                        self._discard_owned_client_index(client_order_index)
                    self._forget_tracked_order(order_id, client_order_index)
                return

            self._lighter_order_fills[order_id] = filled_total

            price = self._to_decimal(update.get("price"))
            base_delta = abs(delta_filled)
            quote_delta = base_delta * price if price > 0 else Decimal("0")

            self._lighter_session_volume_base += base_delta
            if quote_delta > 0:
                self._lighter_session_volume_quote += quote_delta

            direction = Decimal("0")
            if side == "buy":
                direction = Decimal("1")
            elif side == "sell":
                direction = Decimal("-1")

            signed_quantity = base_delta * direction
            if signed_quantity != 0 and side in ("buy", "sell"):
                self._last_fill_timestamp[side] = time.time()
            if signed_quantity != 0 and price > 0:
                self._apply_fill_to_session_pnl(signed_quantity, price)

            if terminal_status:
                self._lighter_order_fills.pop(order_id, None)
                if client_order_index:
                    self._discard_owned_client_index(client_order_index)
                self._forget_tracked_order(order_id, client_order_index)
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.log(f"Failed to process Lighter order update: {exc}", "ERROR")

    def _forget_tracked_order(self, order_id: str, client_order_index: str) -> None:
        for side, tracked in tuple(self._tracked_orders.items()):
            if tracked.order_id == order_id or (
                client_order_index
                and tracked.client_order_index == client_order_index
            ):
                self._tracked_orders.pop(side, None)

    async def _maybe_report_metrics(self, lighter_metrics: Dict[str, Decimal], *, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_metrics_time < self.settings.metrics_interval_seconds:
            return

        self._last_metrics_time = now

        if self._hedger is not None:
            try:
                binance_metrics = await self._hedger.get_account_metrics()
            except Exception as exc:
                self._binance_state_known = False
                self.logger.log(f"Failed to fetch Binance metrics: {exc}", "ERROR")
            else:
                self._binance_state_known = True
                self._binance_position_estimate = binance_metrics.get("position_size", self._binance_position_estimate)
                wallet_balance = binance_metrics.get("wallet_balance", Decimal("0"))
                if self._binance_initial_wallet_balance is None:
                    self._binance_initial_wallet_balance = wallet_balance

                position_notional = binance_metrics.get("position_notional", Decimal("0"))
                position_size = binance_metrics.get("position_size", Decimal("0"))
                position_entry_price = binance_metrics.get("position_entry_price", Decimal("0"))
                position_unrealized = binance_metrics.get("position_unrealized_pnl", Decimal("0"))
                self._binance_inventory_base = position_size
                if position_size == 0:
                    self._binance_avg_entry_price = Decimal("0")
                elif position_entry_price > 0:
                    self._binance_avg_entry_price = position_entry_price
                elif position_notional != 0:
                    try:
                        self._binance_avg_entry_price = abs(position_notional) / abs(position_size)
                    except (InvalidOperation, ZeroDivisionError):
                        pass

                if position_size != 0:
                    mark_candidate: Optional[Decimal] = None
                    if position_entry_price > 0:
                        try:
                            mark_candidate = position_entry_price + (position_unrealized / position_size)
                        except (InvalidOperation, ZeroDivisionError):
                            mark_candidate = None
                    if (mark_candidate is None or mark_candidate <= 0) and position_notional != 0:
                        try:
                            mark_candidate = abs(position_notional) / abs(position_size)
                        except (InvalidOperation, ZeroDivisionError):
                            mark_candidate = None
                    if mark_candidate is not None and mark_candidate > 0:
                        self._binance_last_mark_price = mark_candidate

        lighter_unrealized = self._compute_unrealized_pnl(
            self._lighter_inventory_base,
            self._lighter_avg_entry_price,
            self._lighter_last_mark_price,
        )
        lighter_total = self._lighter_session_realized_pnl + lighter_unrealized

        binance_mark_price = self._binance_last_mark_price
        if binance_mark_price <= 0:
            binance_mark_price = self._lighter_last_mark_price
        binance_unrealized = self._compute_unrealized_pnl(
            self._binance_inventory_base,
            self._binance_avg_entry_price,
            binance_mark_price,
        )
        binance_total = self._binance_session_realized_pnl + binance_unrealized

        combined_total = lighter_total + binance_total

        self.logger.log(
            (
                "Positions | Lighter={lighter_pos} @ {lighter_avg} | Binance={binance_pos} @ {binance_avg}"
            ).format(
                lighter_pos=self._format_decimal(self._lighter_inventory_base, 6),
                lighter_avg=self._format_decimal(self._lighter_avg_entry_price, 4),
                binance_pos=self._format_decimal(self._binance_inventory_base, 6),
                binance_avg=self._format_decimal(self._binance_avg_entry_price, 4),
            ),
            "INFO",
        )

        summary_message = (
            "PnL Summary | Lighter={lighter} | Binance={binance} | Combined={combined}"
        ).format(
            lighter=self._format_decimal(lighter_total, 2),
            binance=self._format_decimal(binance_total, 2),
            combined=self._format_decimal(combined_total, 2),
        )
        self.logger.log(summary_message, "INFO")

        combined_volume_quote = self._lighter_session_volume_quote + self._binance_session_volume_quote
        volume_message = (
            "Volume Summary | Lighter={lighter} | Binance={binance} | Combined={combined}"
        ).format(
            lighter=self._format_decimal(self._lighter_session_volume_quote, 2),
            binance=self._format_decimal(self._binance_session_volume_quote, 2),
            combined=self._format_decimal(combined_volume_quote, 2),
        )
        self.logger.log(volume_message, "INFO")

    @staticmethod
    def _require_env(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise EnvironmentError(f"Environment variable '{name}' is required")
        return value

    def _reset_rate_limit_backoff(self) -> None:
        self._rate_limit_backoff_seconds = self._base_rate_limit_backoff_seconds

    def _handle_iteration_failure(self, exc: Exception) -> Optional[float]:
        if isinstance(exc, asyncio.CancelledError):  # pragma: no cover - handled upstream
            raise

        if self._is_rate_limit_error(exc):
            delay = self._rate_limit_backoff_seconds
            self._rate_limit_backoff_seconds = min(
                self._rate_limit_backoff_seconds * 2,
                self._max_rate_limit_backoff_seconds,
            )
            self.logger.log(
                f"Lighter rate limit encountered; backing off for {delay:.1f}s",
                "WARNING",
            )
            return delay

        if isinstance(exc, aiohttp.ClientError):
            delay = min(self._rate_limit_backoff_seconds, 10.0)
            self.logger.log(
                f"Transient network error during iteration: {exc}; sleeping {delay:.1f}s",
                "WARNING",
            )
            return delay

        status = self._extract_status_code(exc)
        message = str(exc).lower()
        # Lighter occasionally returns 500/50x during engine hiccups; treat these as
        # transient and retry after a short cool-down instead of stopping the loop.
        if status in {500, 502, 503, 504}:
            delay = 5.0
            self.logger.log(
                f"Lighter service error {status}; retrying after {delay:.1f}s",
                "WARNING",
            )
            return delay

        # Signature API sometimes responds with empty/invalid nonce payloads (400),
        # especially when their nonce service is briefly unavailable. A short retry
        # typically succeeds once the upstream recovers.
        if status == 400 and ("invalid nonce" in message or "couldn" in message and "nonce" in message):
            delay = 5.0
            self.logger.log(
                f"Lighter nonce error; retrying after {delay:.1f}s",
                "WARNING",
            )
            return delay

        self.logger.log(f"Iteration failed with unrecoverable error: {exc}", "ERROR")
        return None

    @staticmethod
    def _extract_status_code(exc: BaseException) -> Optional[int]:
        visited = set()
        current: Optional[BaseException] = exc
        while current is not None and id(current) not in visited:
            visited.add(id(current))
            for attr in ("status", "status_code", "code"):
                value = getattr(current, attr, None)
                if isinstance(value, int):
                    return value
            current = current.__cause__ or current.__context__  # type: ignore[assignment]
        return None

    @classmethod
    def _is_rate_limit_error(cls, exc: Exception) -> bool:
        status = cls._extract_status_code(exc)
        if status == 429:
            return True
        message = str(exc)
        return "Too Many Requests" in message or "HTTP 429" in message

    # ------------------------------------------------------------------
    # External control helpers for cluster/agent integrations

    @staticmethod
    def _normalize_allowed_sides(sides: Optional[Iterable[str]]) -> set[str]:
        valid = {"buy", "sell"}
        if not sides:
            return set(valid)
        normalized = {str(side).lower() for side in sides if str(side).lower() in valid}
        return normalized or set(valid)

    @staticmethod
    def _derive_quantity_step(quantity: Decimal) -> Decimal:
        exponent = quantity.as_tuple().exponent
        exponent_int = int(exponent)
        if exponent_int >= 0:
            return Decimal("1")
        scale = Decimal("10") ** (-exponent_int)
        return Decimal("1") / scale

    def _initialize_quantity_range(self, settings: SimpleMakerSettings) -> Optional[tuple[Decimal, Decimal]]:
        min_qty = settings.order_quantity_min
        max_qty = settings.order_quantity_max
        if min_qty is None or max_qty is None:
            return None
        minimum = max(Decimal("0"), min_qty)
        maximum = max(Decimal("0"), max_qty)
        if maximum <= 0:
            return None
        if maximum < minimum:
            minimum, maximum = maximum, minimum
        return (minimum, maximum)

    def set_allowed_sides(self, sides: Iterable[str]) -> None:
        self._allowed_sides = self._normalize_allowed_sides(sides)

    def set_quantity_range(self, minimum: Optional[Decimal], maximum: Optional[Decimal]) -> None:
        if minimum is None or maximum is None:
            self._dynamic_quantity_range = None
            return
        minimum = max(Decimal("0"), minimum)
        maximum = max(Decimal("0"), maximum)
        if maximum < minimum:
            minimum, maximum = maximum, minimum
        self._dynamic_quantity_range = (minimum, maximum)

    def set_random_seed(self, seed: Optional[int]) -> None:
        if seed is None:
            self._rng.seed()
        else:
            self._rng.seed(seed)

    def pause_trading(self) -> None:
        self._external_pause = True

    def resume_trading(self) -> None:
        self._external_pause = False
        self._pause_enforced = False

    def is_paused(self) -> bool:
        return self._external_pause

    async def _flatten_binance_hedge(
        self,
        *,
        tolerance: Decimal,
        max_attempts: int,
    ) -> None:
        if self._hedger is None:
            return

        last_error: Optional[BaseException] = None
        for attempt in range(1, max_attempts + 1):
            try:
                metrics = await self._hedger.get_account_metrics()
                position = self._to_decimal(metrics.get("position_size"))
                self._binance_position_estimate = position
                self._binance_inventory_base = position
                self._binance_state_known = True
                if abs(position) <= tolerance:
                    return

                quantity = await self._hedger.prepare_market_quantity(abs(position))
                if quantity <= 0:
                    raise RuntimeError(
                        f"Binance hedge position {position} is below its executable lot size"
                    )
                side = "SELL" if position > 0 else "BUY"
                response = await self._hedger.place_market_order(
                    side,
                    quantity,
                    reduce_only=True,
                )
                executed = abs(self._to_decimal(response.get("executedQty")))
                if executed <= 0:
                    raise RuntimeError(
                        f"Binance emergency reduce-only order did not execute (attempt {attempt})"
                    )
                fill_price = self._resolve_binance_fill_price(
                    response,
                    executed,
                    self._binance_last_mark_price,
                )
                signed_fill = executed if side == "BUY" else -executed
                if fill_price > 0:
                    self._apply_binance_fill_to_session_pnl(signed_fill, fill_price)
                    self._binance_session_volume_quote += executed * fill_price
                self._binance_session_volume_base += executed
            except Exception as exc:
                last_error = exc
                self._binance_state_known = False
                self.logger.log(
                    f"Binance emergency flatten attempt {attempt}/{max_attempts} failed: {exc}",
                    "ERROR",
                )
                if attempt < max_attempts:
                    await asyncio.sleep(0.25)
                    continue
                raise RuntimeError(
                    f"Binance emergency flatten failed after {max_attempts} attempts: {exc}"
                ) from exc

        try:
            metrics = await self._hedger.get_account_metrics()
            residual = self._to_decimal(metrics.get("position_size"))
        except Exception as exc:
            raise RuntimeError(
                f"Could not verify Binance emergency flatten after {max_attempts} attempts: {exc}"
            ) from exc
        self._binance_position_estimate = residual
        self._binance_inventory_base = residual
        if abs(residual) > tolerance:
            raise RuntimeError(
                f"Binance hedge flatten exhausted {max_attempts} attempts; residual={residual}"
            )

    async def emergency_flatten(
        self,
        *,
        tolerance: Optional[Decimal] = None,
        price_offset_ticks: int = 0,
        max_iterations: Optional[int] = 10,
        sleep_interval: float = 1.5,
    ) -> None:
        if self._lighter_client is None or self._lighter_config is None:
            self.logger.log("Emergency flatten skipped: Lighter client unavailable", "ERROR")
            return

        async with self._flatten_lock:
            if self._flatten_active:
                self.logger.log("Emergency flatten already in progress; ignoring duplicate request", "WARNING")
                return
            self._flatten_active = True
            self.pause_trading()
            operation_lock_acquired = False

            try:
                await self._quote_operation_lock.acquire()
                operation_lock_acquired = True
                tick_size = self._lighter_config.tick_size
                runtime_step = self._runtime_quantity_step() or self._quantity_step
                minimum_tolerance = (
                    runtime_step / Decimal("2")
                    if runtime_step > 0
                    else Decimal("0.00000001")
                )
                if tolerance is None:
                    tol = minimum_tolerance
                else:
                    # A coordinator's generic tolerance must never classify
                    # this maker's entire small inventory as already flat.
                    tol = min(max(Decimal("0"), tolerance), minimum_tolerance)

                self.logger.log(
                    (
                        "Emergency flatten initiated (tolerance={tol}, offset_ticks={offset})"
                    ).format(
                        tol=self._format_decimal(tol, 6),
                        offset=price_offset_ticks,
                    ),
                    "WARNING",
                )

                await self._cancel_all_orders()
                await self._update_state_guarded(force=True)

                attempt_limit = max(1, int(max_iterations or 10))
                order_attempts = 0
                while True:
                    net_position = self._lighter_inventory_base
                    if abs(net_position) <= tol:
                        await self._flatten_binance_hedge(
                            tolerance=tol,
                            max_attempts=attempt_limit,
                        )
                        self.logger.log(
                            (
                                "Emergency flatten complete after {attempt} iterations; residual={residual}"
                            ).format(
                                attempt=order_attempts,
                                residual=self._format_decimal(net_position, 6),
                            ),
                            "WARNING",
                        )
                        break

                    if order_attempts >= attempt_limit:
                        residual = self._format_decimal(net_position, 6)
                        raise RuntimeError(
                            "Emergency flatten max attempts reached; "
                            f"residual Lighter position {residual}"
                        )
                        break

                    side = "sell" if net_position > 0 else "buy"
                    quantity = self._sample_flatten_quantity(net_position)
                    if quantity <= 0:
                        raise RuntimeError(
                            f"Emergency flatten residual {net_position} is not executable"
                        )

                    order_attempts += 1
                    attempt_number = order_attempts
                    try:
                        best_bid, best_ask = await self._lighter_client.fetch_bbo_prices(
                            self._lighter_config.contract_id
                        )
                    except Exception as exc:
                        self.logger.log(f"Failed to load order book during flatten: {exc}", "ERROR")
                        await asyncio.sleep(sleep_interval)
                        await self._update_state_guarded(force=True)
                        continue

                    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                        self.logger.log(
                            "Emergency flatten received an invalid Lighter book; retrying",
                            "ERROR",
                        )
                        await asyncio.sleep(sleep_interval)
                        await self._update_state_guarded(force=True)
                        continue

                    # IOC orders must be marketable: sell into best bid and buy
                    # from best ask. A positive offset increases urgency.
                    price = best_bid if side == "sell" else best_ask

                    if price <= 0:
                        self.logger.log("Emergency flatten aborted: invalid computed price", "ERROR")
                        break

                    offset_ticks = max(0, price_offset_ticks)
                    if tick_size > 0 and offset_ticks > 0:
                        offset = Decimal(offset_ticks) * tick_size
                        if side == "sell":
                            price = max(tick_size, price - offset)
                            price = price.quantize(tick_size, rounding=ROUND_DOWN)
                        else:
                            price = price + offset
                            price = price.quantize(tick_size, rounding=ROUND_UP)

                    try:
                        order_result = await self._lighter_client.place_limit_order(
                            self._lighter_config.contract_id,
                            quantity,
                            price,
                            side,
                            time_in_force="ioc",
                            reduce_only=True,
                        )
                    except Exception as exc:
                        self.logger.log(f"Emergency flatten order exception: {exc}", "ERROR")
                        await asyncio.sleep(sleep_interval)
                        await self._update_state_guarded(force=True)
                        continue

                    if not order_result.success:
                        self.logger.log(
                            (
                                "Emergency flatten order failed: {error}"
                            ).format(error=order_result.error_message or "unknown error"),
                            "ERROR",
                        )
                        await asyncio.sleep(sleep_interval)
                        await self._update_state_guarded(force=True)
                        continue

                    self.logger.log(
                        (
                            "Emergency flatten order submitted ({side}) qty={qty} @ {price} (attempt {attempt})"
                        ).format(
                            side=side,
                            qty=self._format_decimal(quantity, 6),
                            price=self._format_decimal(price, 6),
                            attempt=attempt_number,
                        ),
                        "WARNING",
                    )

                    await asyncio.sleep(max(sleep_interval, self.settings.loop_sleep_seconds))
                    await self._update_state_guarded(force=True)
                    await self._cancel_all_orders()
            finally:
                try:
                    await self._cancel_all_orders()
                finally:
                    if operation_lock_acquired:
                        self._quote_operation_lock.release()
                    self._flatten_active = False
                    self.pause_trading()

    def export_position_snapshot(self) -> Dict[str, str]:
        lighter_position = self._format_decimal(self._lighter_inventory_base, 6)
        binance_position = self._format_decimal(self._binance_position_estimate, 6)
        net_position = self._format_decimal(self._latest_net_position, 6)
        return {
            "lighter_position": lighter_position,
            "binance_position": binance_position,
            "net_position": net_position,
        }

    def export_account_metrics(self) -> Dict[str, str]:
        metrics = getattr(self, "_latest_metrics", {}) or {}

        def _normalize(value: Any) -> Decimal:
            if isinstance(value, Decimal):
                return value
            try:
                return Decimal(str(value))
            except Exception:
                return Decimal("0")

        available = _normalize(metrics.get("available_balance", Decimal("0")))
        total_value = _normalize(metrics.get("total_asset_value", Decimal("0")))

        return {
            "available_balance": self._format_decimal(available, 2),
            "total_asset_value": self._format_decimal(total_value, 2),
        }

    def current_net_position(self) -> Decimal:
        return self._latest_net_position

    def _max_quote_quantity(self) -> Decimal:
        """Return the largest size that the next quote may submit."""

        if self._dynamic_quantity_range is None:
            return abs(self.settings.order_quantity)
        return abs(self._dynamic_quantity_range[1])

    def _runtime_quantity_step(self) -> Optional[Decimal]:
        client = self._lighter_client
        getter = getattr(client, "_spot_size_step", None) if client is not None else None
        if callable(getter):
            try:
                value = getter()
                if isinstance(value, Decimal) and value > 0:
                    return value
            except Exception:
                pass
        return None

    def _normalize_order_quantity(self, quantity: Decimal, price: Decimal) -> Decimal:
        """Align size to live market precision and satisfy base/quote minimums."""

        if quantity <= 0:
            return Decimal("0")
        step = self._runtime_quantity_step() or self._quantity_step
        if step > 0:
            quantity = (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step

        client = self._lighter_client
        min_base = getattr(client, "min_base_amount", None) if client is not None else None
        min_quote = getattr(client, "min_quote_amount", None) if client is not None else None
        required = min_base if isinstance(min_base, Decimal) and min_base > 0 else Decimal("0")
        if isinstance(min_quote, Decimal) and min_quote > 0 and price > 0:
            quote_required = min_quote / price
            if step > 0:
                quote_required = (quote_required / step).to_integral_value(rounding=ROUND_UP) * step
            required = max(required, quote_required)
        if quantity < required:
            quantity = required
        if step > 0 and quantity > 0:
            quantity = (quantity / step).to_integral_value(rounding=ROUND_UP) * step
        return quantity

    def _resolve_order_quantity(self) -> Decimal:
        if self._dynamic_quantity_range is None:
            return self.settings.order_quantity

        lower, upper = self._dynamic_quantity_range
        if upper <= lower:
            return lower

        span = upper - lower
        sample_fraction = Decimal(str(self._rng.random()))
        quantity = lower + (span * sample_fraction)

        if self._quantity_step > 0:
            steps = (quantity / self._quantity_step).to_integral_value(rounding=ROUND_DOWN)
            quantity = steps * self._quantity_step

        if quantity <= 0:
            quantity = lower

        return quantity

    def _sample_flatten_quantity(self, remaining: Decimal) -> Decimal:
        remaining_abs = abs(remaining)
        if remaining_abs <= 0:
            return Decimal("0")

        sampled = self._resolve_order_quantity()
        quantity = min(remaining_abs, sampled)
        if quantity <= 0:
            quantity = remaining_abs

        if self._quantity_step > 0 and quantity > 0:
            steps = (quantity / self._quantity_step).to_integral_value(rounding=ROUND_DOWN)
            if steps > 0:
                quantity = steps * self._quantity_step
            else:
                quantity = remaining_abs

        return quantity


def _parse_args(argv: Optional[Iterable[str]] = None) -> SimpleMakerSettings:
    parser = argparse.ArgumentParser(description="Run a post-only Robinhood Lighter market maker")
    parser.add_argument("--lighter-ticker", default="BTC", help="Lighter market ticker symbol (default: BTC)")
    parser.add_argument("--binance-symbol", default=None, help="Binance Futures signal symbol (default: <ticker>USDT)")
    parser.add_argument(
        "--binance-depth-levels",
        default=10,
        type=int,
        choices=(5, 10, 20, 50, 100),
        help="Binance orderbook levels used for the pressure signal (default: 10)",
    )
    parser.add_argument(
        "--binance-imbalance-max-bps",
        default="3",
        type=_decimal,
        help="Maximum relative quote-center shift from Binance depth (default: 3 bps)",
    )
    parser.add_argument(
        "--bbo-max-distance-ticks",
        default=1,
        type=int,
        help="Maximum distance from local Lighter depth 1 in ticks (default: 1)",
    )
    parser.add_argument("--order-quantity", default="0.00020", type=_decimal, help="Per-order base quantity (default: 0.00020)")
    parser.add_argument("--spread-bps", default="2", type=_decimal, help="Half-spread in basis points (default: 2)")
    parser.add_argument("--hedge-threshold", default="0.001", type=_decimal, help="Inventory threshold for optional hedging (default: 0.001)")
    parser.add_argument("--hedge-buffer", default="0", type=_decimal, help="Buffer deducted from hedge quantity")
    parser.add_argument(
        "--hedge-cooldown-seconds",
        default=30.0,
        type=float,
        help="Minimum seconds between explicit Binance hedge attempts (default: 30)",
    )
    parser.add_argument(
        "--max-hedge-quantity",
        default=None,
        type=_decimal,
        help="Maximum quantity per Binance hedge attempt (default: hedge threshold)",
    )
    parser.add_argument("--inventory-limit", default=None, type=_decimal, help="Inventory cap for pausing one side of quotes")
    parser.add_argument(
        "--inventory-skew-bps",
        default="3",
        type=_decimal,
        help="Maximum quote-center skew at the inventory limit (default: 3 bps)",
    )
    parser.add_argument(
        "--config-path",
        default="configs/robinhood_market_maker.json",
        help="Hot update JSON file or URL",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Credential env file (auto: /etc/perp/robinhood.env, robinhood.env, .env.robinhood, .env)",
    )
    parser.add_argument(
        "--ownership-state-file",
        default=None,
        help="Override the automatic crash-recovery client-order state file",
    )
    parser.add_argument("--lighter-leverage", default=2, type=int, help="Lighter leverage to configure (default: 2)")
    parser.add_argument("--loop-sleep", default=2.0, type=float, help="Seconds between main loop iterations")
    parser.add_argument(
        "--cycles",
        default=0,
        type=int,
        help="Stop and cancel own quotes after N successful iterations (0 = run continuously)",
    )
    parser.add_argument("--order-refresh-ticks", default=2, type=int, help="Price difference in ticks before replacing orders")
    parser.add_argument(
        "--order-refresh-bps",
        default="1",
        type=_decimal,
        help="Minimum price movement in basis points before replacing quotes (default: 1)",
    )
    parser.add_argument(
        "--min-quote-lifetime-seconds",
        default=5.0,
        type=float,
        help="Minimum quote lifetime unless inventory or a 2 bps move requires cancellation",
    )
    parser.add_argument(
        "--order-ack-timeout-seconds",
        default=5.0,
        type=float,
        help="Maximum wait for a submitted quote to appear in private/active-order state",
    )
    parser.add_argument(
        "--binance-reference-timeout-seconds",
        default=1.0,
        type=float,
        help="Maximum wait for one Binance public reference request (default: 1)",
    )
    parser.add_argument(
        "--fill-cooldown-seconds",
        default=5.0,
        type=float,
        help="Seconds to wait after an order is filled before placing a new order on the same side",
    )
    parser.add_argument("--metrics-interval", default=30.0, type=float, help="Seconds between account metrics logs")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging output")
    parser.add_argument(
        "--allowed-side",
        action="append",
        choices=["buy", "sell"],
        dest="allowed_sides",
        help="Restrict quoting to the specified side(s); repeat the flag to allow multiple sides",
    )
    parser.add_argument(
        "--order-quantity-min",
        default=None,
        type=_decimal,
        help="Optional minimum order quantity when sampling random size",
    )
    parser.add_argument(
        "--order-quantity-max",
        default=None,
        type=_decimal,
        help="Optional maximum order quantity when sampling random size",
    )
    parser.add_argument(
        "--enable-binance-hedge",
        action="store_true",
        help="Enable threshold Binance Futures hedging (disabled by default)",
    )
    parser.add_argument(
        "--allow-existing-binance-position",
        action="store_true",
        help="Allow hedge mode to manage an existing Binance position (dedicated account required)",
    )
    parser.add_argument(
        "--disable-binance-hedge",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--disable-binance-reference",
        action="store_true",
        help="Disable the Binance orderbook pressure signal and use only the local Lighter midpoint",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.enable_binance_hedge and args.disable_binance_hedge:
        parser.error("--enable-binance-hedge and --disable-binance-hedge are mutually exclusive")
    if args.order_quantity <= 0:
        parser.error("--order-quantity must be positive")
    if args.spread_bps <= 0:
        parser.error("--spread-bps must be positive")
    if args.bbo_max_distance_ticks < 0:
        parser.error("--bbo-max-distance-ticks must not be negative")
    if args.hedge_threshold <= 0:
        parser.error("--hedge-threshold must be positive")
    if args.hedge_buffer < 0:
        parser.error("--hedge-buffer must not be negative")
    if args.hedge_buffer >= args.hedge_threshold:
        parser.error("--hedge-buffer must be smaller than --hedge-threshold")
    if args.hedge_cooldown_seconds < 0:
        parser.error("--hedge-cooldown-seconds must not be negative")
    if args.max_hedge_quantity is not None and args.max_hedge_quantity <= 0:
        parser.error("--max-hedge-quantity must be positive")
    if args.inventory_limit is not None and args.inventory_limit <= 0:
        parser.error("--inventory-limit must be positive")
    if args.inventory_skew_bps < 0:
        parser.error("--inventory-skew-bps must not be negative")
    if args.binance_imbalance_max_bps < 0:
        parser.error("--binance-imbalance-max-bps must not be negative")
    if args.lighter_leverage <= 0:
        parser.error("--lighter-leverage must be positive")
    if args.loop_sleep <= 0:
        parser.error("--loop-sleep must be positive")
    if args.cycles < 0:
        parser.error("--cycles must not be negative")
    if args.order_refresh_ticks <= 0:
        parser.error("--order-refresh-ticks must be positive")
    if args.order_refresh_bps < 0:
        parser.error("--order-refresh-bps must not be negative")
    if args.min_quote_lifetime_seconds < 0:
        parser.error("--min-quote-lifetime-seconds must not be negative")
    if args.order_ack_timeout_seconds <= 0:
        parser.error("--order-ack-timeout-seconds must be positive")
    if args.binance_reference_timeout_seconds <= 0:
        parser.error("--binance-reference-timeout-seconds must be positive")
    if args.fill_cooldown_seconds < 0:
        parser.error("--fill-cooldown-seconds must not be negative")
    if args.order_quantity_min is not None and args.order_quantity_min <= 0:
        parser.error("--order-quantity-min must be positive")
    if args.order_quantity_max is not None and args.order_quantity_max <= 0:
        parser.error("--order-quantity-max must be positive")
    if (
        args.order_quantity_min is not None
        and args.order_quantity_max is not None
        and args.order_quantity_min > args.order_quantity_max
    ):
        parser.error("--order-quantity-min must not exceed --order-quantity-max")
    lighter_ticker = str(args.lighter_ticker).upper()
    binance_symbol = str(args.binance_symbol or f"{lighter_ticker}USDT").upper()
    return SimpleMakerSettings(
        lighter_ticker=lighter_ticker,
        binance_symbol=binance_symbol,
        order_quantity=args.order_quantity,
        base_spread_bps=args.spread_bps,
        hedge_threshold=args.hedge_threshold,
        hedge_buffer=args.hedge_buffer,
        hedge_cooldown_seconds=max(0.0, args.hedge_cooldown_seconds),
        max_hedge_quantity=(
            max(Decimal("0"), args.max_hedge_quantity)
            if args.max_hedge_quantity is not None
            else args.hedge_threshold
        ),
        enable_binance_hedge=bool(args.enable_binance_hedge and not args.disable_binance_hedge),
        inventory_limit=args.inventory_limit,
        config_path=args.config_path,
        env_file=args.env_file,
        lighter_environment="robinhood",
        lighter_leverage=args.lighter_leverage,
        loop_sleep_seconds=args.loop_sleep,
        max_cycles=args.cycles,
        order_refresh_ticks=args.order_refresh_ticks,
        order_refresh_bps=args.order_refresh_bps,
        min_quote_lifetime_seconds=args.min_quote_lifetime_seconds,
        order_ack_timeout_seconds=args.order_ack_timeout_seconds,
        binance_reference_timeout_seconds=args.binance_reference_timeout_seconds,
        binance_depth_levels=args.binance_depth_levels,
        binance_imbalance_max_bps=args.binance_imbalance_max_bps,
        bbo_max_distance_ticks=args.bbo_max_distance_ticks,
        fill_cooldown_seconds=args.fill_cooldown_seconds,
        log_to_console=not args.no_console_log,
        metrics_interval_seconds=max(5.0, args.metrics_interval),
        allowed_sides=frozenset(args.allowed_sides) if args.allowed_sides else None,
        order_quantity_min=args.order_quantity_min,
        order_quantity_max=args.order_quantity_max,
        use_binance_reference=not args.disable_binance_reference,
        inventory_skew_bps=args.inventory_skew_bps,
        ownership_state_path=args.ownership_state_file,
        allow_existing_binance_position=args.allow_existing_binance_position,
    )


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stopper: asyncio.Event) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopper.set)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            signal.signal(sig, lambda *_: stopper.set())


async def _async_main(settings: SimpleMakerSettings) -> None:
    async with SimpleMarketMaker(settings) as maker:
        loop = asyncio.get_running_loop()
        stopper = asyncio.Event()
        _install_signal_handlers(loop, stopper)

        run_task = asyncio.create_task(maker.run())
        stop_task = asyncio.create_task(stopper.wait())
        done, pending = await asyncio.wait(
            {run_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        shutdown_reason = "Maker loop completed"
        run_error: Optional[BaseException] = None
        if run_task in done:
            try:
                await run_task
            except Exception as exc:  # noqa: BLE001
                run_error = exc
                shutdown_reason = f"Maker loop crashed: {exc}"
        if stop_task in done and run_task not in done:
            shutdown_reason = "Shutdown requested via signal"

        maker.logger.log(f"{shutdown_reason}; stopping maker", "WARNING")

        if run_error is not None:
            _LOGGER.exception("Maker loop terminated due to error", exc_info=run_error)
            raise run_error
        for task in done:
            if task is run_task:
                continue
            with contextlib.suppress(Exception):
                await task


def main(argv: Optional[Iterable[str]] = None) -> None:
    settings = _parse_args(argv)
    try:
        asyncio.run(_async_main(settings))
    except KeyboardInterrupt:  # pragma: no cover - CLI convenience
        _LOGGER.warning("Interrupted by user")


if __name__ == "__main__":
    main()
