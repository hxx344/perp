"""Minimal Lighter market-making loop with Binance hedging.

This module keeps a single bid/ask resting on Lighter, adjusts them according to
mid-price and a static spread, and hedges net exposure on Binance Futures once
an inventory threshold is breached. Hot-update configuration is reloaded each
loop iteration so ops can pause the cycle or tweak parameters without restarts.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hmac
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, cast
from urllib.parse import urlencode

import aiohttp
import dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

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
    inventory_limit: Optional[Decimal] = None
    config_path: str = "configs/hot_update.json"
    env_file: Optional[str] = None
    loop_sleep_seconds: float = 3.0
    order_refresh_ticks: int = 2
    log_to_console: bool = True
    metrics_interval_seconds: float = 30.0

    def effective_inventory_limit(self) -> Decimal:
        return self.inventory_limit if self.inventory_limit is not None else self.hedge_threshold


@dataclass(slots=True)
class ActiveOrder:
    """Tracked order metadata for a single side."""

    order_id: str
    price: Decimal
    side: str


def compute_target_prices(
    mid_price: Decimal,
    spread_bps: Decimal,
    tick_size: Decimal,
) -> Dict[str, Decimal]:
    """Return rounded bid/ask targets based on mid price and spread."""
    half_spread = (mid_price * spread_bps / Decimal("10000")).quantize(tick_size, rounding=ROUND_HALF_UP)
    if half_spread < tick_size:
        half_spread = tick_size
    bid_price = (mid_price - half_spread).quantize(tick_size, rounding=ROUND_HALF_UP)
    ask_price = (mid_price + half_spread).quantize(tick_size, rounding=ROUND_HALF_UP)
    return {"buy": bid_price, "sell": ask_price}


def should_enable_side(net_position: Decimal, limit: Decimal, side: str) -> bool:
    """Check if quoting for a side should remain enabled under inventory constraints."""
    if side == "buy":
        return net_position <= limit
    return net_position >= -limit


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

    async def place_market_order(self, side: str, quantity: Decimal) -> Dict[str, Any]:
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


class SimpleMarketMaker:
    """Run a lightweight maker loop on Lighter with threshold hedging on Binance."""

    def __init__(self, settings: SimpleMakerSettings) -> None:
        self.settings = settings
        self.logger = TradingLogger("lighter-simple", settings.lighter_ticker, log_to_console=settings.log_to_console)
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._hedger: Optional[BinanceHedger] = None
        self._lighter_client: Optional[LighterClient] = None
        self._lighter_config: Optional[TradingConfig] = None
        self._tracked_orders: Dict[str, ActiveOrder] = {}
        self._last_hot_update: Dict[str, Any] = {}
        self._last_metrics_time: float = 0.0
        self._lighter_order_fills: Dict[str, Decimal] = {}
        self._lighter_session_volume_quote: Decimal = Decimal("0")
        self._lighter_session_volume_base: Decimal = Decimal("0")
        self._binance_position_estimate: Decimal = Decimal("0")
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
        self._state_update_lock = asyncio.Lock()
        self._binance_session_realized_pnl: Decimal = Decimal("0")
        self._binance_inventory_base: Decimal = Decimal("0")
        self._binance_avg_entry_price: Decimal = Decimal("0")
        self._binance_last_mark_price: Decimal = Decimal("0")
        self._binance_session_volume_quote: Decimal = Decimal("0")
        self._binance_session_volume_base: Decimal = Decimal("0")

    async def __aenter__(self) -> "SimpleMarketMaker":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._running:
            return

        env_path = self.settings.env_file
        if env_path:
            loaded = dotenv.load_dotenv(env_path)
            if loaded:
                self.logger.log(f"Loaded environment variables from '{env_path}'", "INFO")
            else:
                self.logger.log(
                    f"Env file '{env_path}' not found or empty; using existing process environment",
                    "WARNING",
                )
        else:
            dotenv.load_dotenv()

        timeout = aiohttp.ClientTimeout(total=15)
        self._session = aiohttp.ClientSession(timeout=timeout)

        api_key = self._require_env("BINANCE_API_KEY")
        api_secret = self._require_env("BINANCE_API_SECRET")
        self._hedger = BinanceHedger(api_key, api_secret, self.settings.binance_symbol, self._session)

        initial_binance_position = Decimal("0")
        initial_binance_avg_price = Decimal("0")
        initial_binance_mark = Decimal("0")
        try:
            hedger_snapshot = await self._hedger.get_account_metrics()
            position_size = hedger_snapshot.get("position_size", Decimal("0"))
            position_notional = hedger_snapshot.get("position_notional", Decimal("0"))
            position_entry_price = hedger_snapshot.get("position_entry_price", Decimal("0"))
            position_unrealized = hedger_snapshot.get("position_unrealized_pnl", Decimal("0"))
            self._binance_position_estimate = position_size
            initial_binance_position = position_size
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
            self.logger.log(f"Failed to seed Binance position estimate: {exc}", "WARNING")
            self._binance_position_estimate = Decimal("0")
            self._binance_initial_wallet_balance = None

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

        self._lighter_config = trading_config
        lighter_client = cast(
            LighterClient,
            ExchangeFactory.create_exchange("lighter", trading_config),  # type: ignore[arg-type]
        )
        lighter_client.setup_order_update_handler(self._handle_lighter_order_update)
        self._lighter_client = lighter_client
        await self._lighter_client.connect()
        contract_id, tick_size = await self._lighter_client.get_contract_attributes()
        trading_config.contract_id = contract_id
        trading_config.tick_size = tick_size
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

    async def _configure_lighter_leverage(self) -> None:
        if self._lighter_client is None:
            return

        leverage_limits: Dict[str, Optional[int]] = {}
        limits_getter = getattr(self._lighter_client, "get_leverage_limits", None)
        if callable(limits_getter):
            try:
                leverage_limits = cast(Dict[str, Optional[int]], limits_getter())
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.log(f"Failed to load Lighter leverage limits: {exc}", "WARNING")
                return
        else:  # pragma: no cover - unexpected SDK change
            self.logger.log(
                "Current Lighter client does not expose leverage metadata; skipping auto configuration",
                "WARNING",
            )
            return

        max_leverage = leverage_limits.get("max")
        default_leverage = leverage_limits.get("default")
        if max_leverage in (None, 0):
            self.logger.log(
                "Unable to determine Lighter max leverage from market metadata; set leverage manually if needed",
                "WARNING",
            )
            return

        try:
            target_leverage = int(max_leverage)
        except (TypeError, ValueError):
            self.logger.log(f"Invalid Lighter leverage limit received: {max_leverage}", "WARNING")
            return

        if target_leverage <= 0:
            self.logger.log(f"Ignoring non-positive Lighter leverage limit: {target_leverage}", "WARNING")
            return

        default_display = str(default_leverage) if default_leverage is not None else "unknown"
        self.logger.log(
            f"Targeting Lighter max leverage {target_leverage}x (default {default_display}x)",
            "INFO",
        )

        await self._ensure_lighter_leverage(target_leverage)

    async def _ensure_lighter_leverage(self, leverage: int) -> None:
        if leverage <= 0:
            return
        if self._lighter_client is None or self._lighter_config is None:
            self.logger.log("Cannot update Lighter leverage: client not initialized", "WARNING")
            return

        signer_client = getattr(self._lighter_client, "lighter_client", None)
        if signer_client is None:
            self.logger.log("Cannot update Lighter leverage: signer client unavailable", "WARNING")
            return

        contract_id = getattr(self._lighter_config, "contract_id", None)
        if contract_id is None:
            self.logger.log("Cannot update Lighter leverage: contract id not resolved", "ERROR")
            return
        try:
            market_index = int(contract_id)
        except (TypeError, ValueError):
            self.logger.log(f"Cannot update Lighter leverage: invalid contract id '{contract_id}'", "ERROR")
            return

        margin_mode = getattr(signer_client, "CROSS_MARGIN_MODE", None)
        if margin_mode is None:
            self.logger.log("Cannot update Lighter leverage: margin mode unavailable", "WARNING")
            return

        try:
            tx_info, _, err = await signer_client.update_leverage(
                market_index,
                margin_mode,
                int(leverage),
            )
        except Exception as exc:  # pragma: no cover - network/SDK failures
            self.logger.log(f"Failed to update Lighter leverage to {leverage}x: {exc}", "ERROR")
            return

        if err is not None:
            message = str(err)
            lowered = message.lower()
            if "same" in lowered or "already" in lowered:
                self.logger.log(
                    f"Lighter leverage already set to {leverage}x; no change required",
                    "INFO",
                )
                return

            self.logger.log(f"Failed to update Lighter leverage to {leverage}x: {message}", "ERROR")
            return

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
        if not self._running:
            await self._shutdown_state_task()
            if self._session and not self._session.closed:
                await self._session.close()
            return

        self._running = False
        await self._shutdown_state_task()

        try:
            if self._lighter_client is not None:
                await self._lighter_client.disconnect()
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    async def run(self) -> None:
        if not self._running:
            raise RuntimeError("SimpleMarketMaker.start() must be called first")

        try:
            while self._running:
                try:
                    await self._iteration()
                except Exception as exc:  # noqa: BLE001
                    delay = self._handle_iteration_failure(exc)
                    if delay is None:
                        raise
                    await asyncio.sleep(delay)
                    continue

                self._reset_rate_limit_backoff()
                await asyncio.sleep(self.settings.loop_sleep_seconds)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            self.logger.log("Maker loop cancelled", "WARNING")
        finally:
            await self.stop()

    async def _iteration(self) -> None:
        await self._refresh_quotes()

    async def _refresh_quotes(self) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None

        hot_update = await self._load_hot_update()
        if not hot_update.get("cycle_enabled", True):
            self.logger.log("Cycle paused via hot update; sleeping", "WARNING")
            self._tracked_orders.clear()
            await self._cancel_all_orders()
            return

        contract_id = self._lighter_config.contract_id
        best_bid, best_ask = await self._lighter_client.fetch_bbo_prices(contract_id)

        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log("Invalid Lighter depth snapshot; skipping iteration", "WARNING")
            return

        now = time.time()
        max_age = self._state_refresh_interval * 2.0
        if (now - self._latest_net_position_time) > max_age:
            await self._refresh_state_if_needed(max_age=max_age)

        net_position = self._latest_net_position

        mid_price = (best_bid + best_ask) / 2
        self._lighter_last_mark_price = mid_price
        spread_scale = self._resolve_spread_scale(hot_update)
        targets = compute_target_prices(mid_price, spread_scale, self._lighter_config.tick_size)

        inventory_cap = self.settings.effective_inventory_limit()
        bid_enabled = should_enable_side(net_position, inventory_cap, "buy")
        ask_enabled = should_enable_side(net_position, inventory_cap, "sell")

        await self._sync_side("buy", targets["buy"], bid_enabled)
        await self._sync_side("sell", targets["sell"], ask_enabled)

        await self._maybe_execute_hedge(net_position)

    async def _sync_side(self, side: str, target_price: Decimal, enabled: bool) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None

        active_orders = await self._lighter_client.get_active_orders(self._lighter_config.contract_id)
        relevant_orders = [order for order in active_orders if order.side == side]
        replace_threshold = self._lighter_config.tick_size * Decimal(self.settings.order_refresh_ticks)

        kept: Iterable[ActiveOrder] = ()
        for idx, order in enumerate(relevant_orders):
            price_diff = abs(order.price - target_price)
            keep = enabled and idx == 0 and price_diff <= replace_threshold
            if keep:
                self._tracked_orders[side] = ActiveOrder(order_id=order.order_id, price=order.price, side=side)
                kept = (self._tracked_orders[side],)
                continue
            try:
                await self._lighter_client.cancel_order(order.order_id)
                self.logger.log(f"Cancelled stale {side} order {order.order_id}", "INFO")
            except Exception as exc:
                self.logger.log(f"Failed to cancel order {order.order_id}: {exc}", "ERROR")

        if not enabled:
            if side in self._tracked_orders:
                del self._tracked_orders[side]
            return

        if kept:
            return

        order_result = await self._lighter_client.place_limit_order(
            self._lighter_config.contract_id,
            self.settings.order_quantity,
            target_price,
            side,
        )
        if not order_result.success:
            self.logger.log(f"Failed to place {side} order: {order_result.error_message}", "ERROR")
            return
        price_display = self._format_decimal(target_price, 6)
        self.logger.log(
            (
                "Submitted {side} order request client_id={client_id} @ {price}"
            ).format(side=side, client_id=order_result.order_id, price=price_display),
            "INFO",
        )

    async def _maybe_execute_hedge(self, net_position: Decimal) -> None:
        if self._hedger is None:
            return

        combined_position = net_position + self._binance_position_estimate
        hedge_qty = required_hedge_quantity(
            combined_position,
            self.settings.hedge_threshold,
            self.settings.hedge_buffer,
        )
        if hedge_qty <= 0:
            return

        hedge_side = "SELL" if combined_position > 0 else "BUY"
        raw_hedge_qty = hedge_qty
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
            executed_qty = self._to_decimal(
                order_response.get("executedQty")
                or order_response.get("cumQty")
                or order_response.get("origQty")
                or hedge_qty
            )
            if executed_qty <= 0:
                executed_qty = hedge_qty

            signed_qty = executed_qty if hedge_side == "BUY" else -executed_qty
            fill_price = self._resolve_binance_fill_price(
                order_response,
                executed_qty,
                self._lighter_last_mark_price,
            )
            self._binance_position_estimate += signed_qty
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
                position_size = Decimal("0")
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

    async def _cancel_all_orders(self) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None
        active_orders = await self._lighter_client.get_active_orders(self._lighter_config.contract_id)
        for order in active_orders:
            try:
                await self._lighter_client.cancel_order(order.order_id)
            except Exception as exc:
                self.logger.log(f"Failed to cancel order {order.order_id}: {exc}", "ERROR")
        self._tracked_orders.clear()

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
        return self._last_hot_update or {"cycle_enabled": True}

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
            if status not in {"FILLED", "CANCELED", "PARTIALLY_FILLED"}:
                return

            order_id = str(update.get("order_id") or "")
            if not order_id:
                return

            filled_total = abs(self._to_decimal(update.get("filled_size")))
            previous_filled = self._lighter_order_fills.get(order_id, Decimal("0"))
            delta_filled = filled_total - previous_filled
            if delta_filled <= 0:
                if status in {"FILLED", "CANCELED"}:
                    self._lighter_order_fills.pop(order_id, None)
                return

            self._lighter_order_fills[order_id] = filled_total

            price = self._to_decimal(update.get("price"))
            base_delta = abs(delta_filled)
            quote_delta = base_delta * price if price > 0 else Decimal("0")

            self._lighter_session_volume_base += base_delta
            if quote_delta > 0:
                self._lighter_session_volume_quote += quote_delta

            side = str(update.get("side") or "").lower()
            direction = Decimal("0")
            if side == "buy":
                direction = Decimal("1")
            elif side == "sell":
                direction = Decimal("-1")

            signed_quantity = base_delta * direction
            if signed_quantity != 0 and price > 0:
                self._apply_fill_to_session_pnl(signed_quantity, price)

            if status in {"FILLED", "CANCELED"}:
                self._lighter_order_fills.pop(order_id, None)
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.log(f"Failed to process Lighter order update: {exc}", "ERROR")

    async def _maybe_report_metrics(self, lighter_metrics: Dict[str, Decimal], *, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_metrics_time < self.settings.metrics_interval_seconds:
            return

        self._last_metrics_time = now

        if self._hedger is not None:
            try:
                binance_metrics = await self._hedger.get_account_metrics()
            except Exception as exc:
                self.logger.log(f"Failed to fetch Binance metrics: {exc}", "ERROR")
            else:
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


def _parse_args(argv: Optional[Iterable[str]] = None) -> SimpleMakerSettings:
    parser = argparse.ArgumentParser(description="Run a minimal Lighter market maker with Binance hedging")
    parser.add_argument("--lighter-ticker", required=True, help="Lighter market ticker symbol (e.g. ETH-PERP)")
    parser.add_argument("--binance-symbol", required=True, help="Binance futures symbol (e.g. ETHUSDT)")
    parser.add_argument("--order-quantity", required=True, type=_decimal, help="Per-order base quantity")
    parser.add_argument("--spread-bps", required=True, type=_decimal, help="Half-spread in basis points")
    parser.add_argument("--hedge-threshold", required=True, type=_decimal, help="Inventory threshold that triggers hedging")
    parser.add_argument("--hedge-buffer", default="0", type=_decimal, help="Buffer deducted from hedge quantity")
    parser.add_argument("--inventory-limit", default=None, type=_decimal, help="Inventory cap for pausing one side of quotes")
    parser.add_argument("--config-path", default="configs/hot_update.json", help="Hot update JSON file or URL")
    parser.add_argument("--env-file", default=None, help="Optional path to a .env file to load before starting")
    parser.add_argument("--loop-sleep", default=3.0, type=float, help="Seconds between main loop iterations")
    parser.add_argument("--order-refresh-ticks", default=2, type=int, help="Price difference in ticks before replacing orders")
    parser.add_argument("--metrics-interval", default=30.0, type=float, help="Seconds between account metrics logs")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging output")

    args = parser.parse_args(list(argv) if argv is not None else None)
    return SimpleMakerSettings(
        lighter_ticker=args.lighter_ticker,
        binance_symbol=args.binance_symbol,
        order_quantity=args.order_quantity,
        base_spread_bps=args.spread_bps,
        hedge_threshold=args.hedge_threshold,
        hedge_buffer=args.hedge_buffer,
        inventory_limit=args.inventory_limit,
        config_path=args.config_path,
        env_file=args.env_file,
        loop_sleep_seconds=args.loop_sleep,
        order_refresh_ticks=max(1, args.order_refresh_ticks),
        log_to_console=not args.no_console_log,
        metrics_interval_seconds=max(5.0, args.metrics_interval),
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
        await maker.stop()

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
