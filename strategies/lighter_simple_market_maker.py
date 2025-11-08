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
from decimal import Decimal, ROUND_HALF_UP
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

    def _sign(self, params: Dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        signature = hmac.new(self.api_secret, query.encode(), sha256).hexdigest()
        return signature

    async def place_market_order(self, side: str, quantity: Decimal) -> Dict[str, Any]:
        timestamp = int(time.time() * 1000)
        params = {
            "symbol": self.symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": format(quantity, "f"),
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
        }

        return metrics


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

    async def __aenter__(self) -> "SimpleMarketMaker":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._running:
            return

        dotenv.load_dotenv()
        timeout = aiohttp.ClientTimeout(total=15)
        self._session = aiohttp.ClientSession(timeout=timeout)

        api_key = self._require_env("BINANCE_API_KEY")
        api_secret = self._require_env("BINANCE_API_SECRET")
        self._hedger = BinanceHedger(api_key, api_secret, self.settings.binance_symbol, self._session)

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
        await self._lighter_client.wait_for_market_data(timeout=10)

        self.logger.log(
            f"Initialized simple market maker: contract={contract_id}, tick_size={tick_size}, "
            f"order_qty={self.settings.order_quantity}",
            "INFO",
        )
        self._running = True

    async def stop(self) -> None:
        if not self._running:
            if self._session and not self._session.closed:
                await self._session.close()
            return

        try:
            if self._lighter_client is not None:
                await self._lighter_client.disconnect()
        finally:
            if self._session and not self._session.closed:
                await self._session.close()
            self._running = False

    async def run(self) -> None:
        if not self._running:
            raise RuntimeError("SimpleMarketMaker.start() must be called first")

        try:
            while self._running:
                await self._iteration()
                await asyncio.sleep(self.settings.loop_sleep_seconds)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            self.logger.log("Maker loop cancelled", "WARNING")
        finally:
            await self.stop()

    async def _iteration(self) -> None:
        assert self._lighter_client is not None
        assert self._lighter_config is not None

        hot_update = await self._load_hot_update()
        if not hot_update.get("cycle_enabled", True):
            self.logger.log("Cycle paused via hot update; sleeping", "WARNING")
            self._tracked_orders.clear()
            await self._cancel_all_orders()
            return

        best_bid, best_ask = await self._lighter_client.fetch_bbo_prices(self._lighter_config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log("Invalid Lighter depth snapshot; skipping iteration", "WARNING")
            return

        mid_price = (best_bid + best_ask) / 2
        spread_scale = self._resolve_spread_scale(hot_update)
        targets = compute_target_prices(mid_price, spread_scale, self._lighter_config.tick_size)

        try:
            lighter_metrics = await self._lighter_client.get_account_metrics()
        except Exception as exc:
            self.logger.log(f"Failed to fetch Lighter account metrics: {exc}", "ERROR")
            lighter_metrics = {
                "position_size": await self._lighter_client.get_account_positions(),
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

        net_position = lighter_metrics.get("position_size", Decimal("0"))
        inventory_cap = self.settings.effective_inventory_limit()

        bid_enabled = should_enable_side(net_position, inventory_cap, "buy")
        ask_enabled = should_enable_side(net_position, inventory_cap, "sell")

        await self._sync_side("buy", targets["buy"], bid_enabled)
        await self._sync_side("sell", targets["sell"], ask_enabled)

        hedge_qty = required_hedge_quantity(net_position, self.settings.hedge_threshold, self.settings.hedge_buffer)
        if hedge_qty > 0 and self._hedger is not None:
            hedge_side = "SELL" if net_position > 0 else "BUY"
            try:
                await self._hedger.place_market_order(hedge_side, hedge_qty)
                self.logger.log(
                    f"Executed Binance hedge: side={hedge_side}, qty={hedge_qty} (net_position={net_position})",
                    "INFO",
                )
            except Exception as exc:
                self.logger.log(f"Binance hedge failed: {exc}", "ERROR")

        await self._maybe_report_metrics(lighter_metrics)

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

        # Refresh active orders so we can store actual order index
        await asyncio.sleep(0.2)
        active_orders = await self._lighter_client.get_active_orders(self._lighter_config.contract_id)
        for order in active_orders:
            if order.side == side and abs(order.price - target_price) <= self._lighter_config.tick_size:
                self._tracked_orders[side] = ActiveOrder(order_id=order.order_id, price=order.price, side=side)
                self.logger.log(f"Placed {side} order {order.order_id} @ {order.price}", "INFO")
                break

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

            if status in {"FILLED", "CANCELED"}:
                self._lighter_order_fills.pop(order_id, None)
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.log(f"Failed to process Lighter order update: {exc}", "ERROR")

    async def _maybe_report_metrics(self, lighter_metrics: Dict[str, Decimal]) -> None:
        now = time.time()
        if now - self._last_metrics_time < self.settings.metrics_interval_seconds:
            return

        self._last_metrics_time = now

        session_volume = self._lighter_session_volume_quote

        lighter_message = (
            "Lighter | pos={pos} | posVal={pos_val} | uPnL={u_pnl} | rPnL={r_pnl} | bal={bal} | "
            "dailyVol={daily} | sessionVol={session}"
        ).format(
            pos=self._format_decimal(lighter_metrics.get("position_size", Decimal("0")), 6),
            pos_val=self._format_decimal(lighter_metrics.get("position_value", Decimal("0")), 2),
            u_pnl=self._format_decimal(lighter_metrics.get("unrealized_pnl", Decimal("0")), 2),
            r_pnl=self._format_decimal(lighter_metrics.get("realized_pnl", Decimal("0")), 2),
            bal=self._format_decimal(lighter_metrics.get("available_balance", Decimal("0")), 2),
            daily=self._format_decimal(lighter_metrics.get("daily_volume", Decimal("0")), 6),
            session=self._format_decimal(session_volume, 6),
        )
        self.logger.log(lighter_message, "INFO")

        if self._hedger is None:
            return

        try:
            binance_metrics = await self._hedger.get_account_metrics()
        except Exception as exc:
            self.logger.log(f"Failed to fetch Binance metrics: {exc}", "ERROR")
            return

        binance_message = (
            "Binance | pos={pos} | notional={notional} | uPnL={u_pnl} | wallet={wallet} | avail={avail}"
        ).format(
            pos=self._format_decimal(binance_metrics.get("position_size", Decimal("0")), 6),
            notional=self._format_decimal(binance_metrics.get("position_notional", Decimal("0")), 2),
            u_pnl=self._format_decimal(binance_metrics.get("position_unrealized_pnl", Decimal("0")), 2),
            wallet=self._format_decimal(binance_metrics.get("wallet_balance", Decimal("0")), 2),
            avail=self._format_decimal(binance_metrics.get("available_balance", Decimal("0")), 2),
        )
        self.logger.log(binance_message, "INFO")

    @staticmethod
    def _require_env(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise EnvironmentError(f"Environment variable '{name}' is required")
        return value


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
