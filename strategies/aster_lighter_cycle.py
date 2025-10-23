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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING, Tuple, cast

import aiohttp

import dotenv

from exchanges import ExchangeFactory
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


class HedgingCycleExecutor:
    """Coordinates the four-leg hedging cycle between Aster and Lighter."""

    def __init__(self, config: CycleConfig):
        self.config = config
        ticker_label = f"{config.aster_ticker}_{config.lighter_ticker}".replace("/", "-")
        self.logger = TradingLogger(exchange="hedge", ticker=ticker_label, log_to_console=True)

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
        self._last_leg1_price: Optional[Decimal] = None
        self._housekeeping_task: Optional[asyncio.Task] = None

    async def _memory_housekeeping_loop(self) -> None:
        """Periodically trigger GC and clean bounded in-memory states.

        Controlled by env vars:
        - MEMORY_CLEAN_INTERVAL_SECONDS: interval seconds (default 300)
        - MEMORY_WARN_MB: if >0, warn when rss exceeds threshold (best-effort)
        """
        interval_s = float(os.getenv("MEMORY_CLEAN_INTERVAL_SECONDS", "300") or 300)
        warn_mb = float(os.getenv("MEMORY_WARN_MB", "0") or 0)
        # Clamp interval to sensible bounds
        interval_s = max(30.0, min(interval_s, 3600.0))

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
                        "DEBUG",
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

            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Don't let housekeeping kill the executor
                self.logger.log(f"Housekeeping loop error: {exc}", "WARNING")
                continue

    def _lighter_initial_direction(self) -> str:
        return "sell" if self.config.direction == "buy" else "buy"

    async def setup(self) -> None:
        """Instantiate exchange clients, connect, and hydrate contract metadata."""
        aster_client = ExchangeFactory.create_exchange("aster", self.aster_config)  # type: ignore[arg-type]
        lighter_client = ExchangeFactory.create_exchange("lighter", self.lighter_config)  # type: ignore[arg-type]
        self.aster_client = cast("AsterClient", aster_client)
        self.lighter_client = cast("LighterClient", lighter_client)

        await self.aster_client.connect()
        await self.lighter_client.connect()

        aster_contract_id, aster_tick = await self.aster_client.get_contract_attributes()
        lighter_contract_id, lighter_tick = await self.lighter_client.get_contract_attributes()

        self.logger.log(
            (
                f"Contracts resolved | Aster: id={aster_contract_id}, tick={_format_decimal(aster_tick, 8)}; "
                f"Lighter: id={lighter_contract_id}, tick={_format_decimal(lighter_tick, 8)}"
            ),
            "INFO",
        )

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
        if not self.aster_client or not self.lighter_client:
            raise RuntimeError("Exchange clients are not initialized. Call setup() first.")

        results: List[LegResult] = []

        # Leg 1: Aster maker entry
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
        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        if self.config.virtual_aster_maker:
            return await self._simulate_virtual_aster_leg(
                leg_name=leg_name,
                direction=direction,
                quantity=self.config.aster_quantity,
            )

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
        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        if self.config.virtual_aster_maker:
            return await self._simulate_virtual_aster_leg(
                leg_name=leg_name,
                direction=direction,
                quantity=self.config.aster_quantity,
            )

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
        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

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
        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        contract_id = self.aster_config.contract_id
        if not contract_id and not (self._virtual_price_source == "bn" and self._binance_price_client):
            raise RuntimeError("Aster contract id is not initialized")

        deadline = time.time() + self.config.max_wait_seconds
        poll_interval = max(self.config.poll_interval, 0.05)

        while True:
            if self._virtual_price_source == "bn" and self._binance_price_client:
                best_bid, best_ask = await self._binance_price_client.fetch_bbo_prices()
            else:
                best_bid, best_ask = await self.aster_client.fetch_bbo_prices(contract_id)
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

    

    async def _calculate_aster_maker_price(self, direction: str) -> Decimal:
        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        tick = self.aster_config.tick_size if self.aster_config.tick_size > 0 else Decimal("0")
        level = 4

        depth_price, best_bid, best_ask = await self.aster_client.get_depth_level_price(
            self.aster_config.contract_id,
            direction,
            level,
        )

        best_bid = best_bid if best_bid is not None else Decimal("0")
        best_ask = best_ask if best_ask is not None else Decimal("0")

        # If no usable depth price, fetch fallback BBO
        if depth_price is None or depth_price <= 0:
            fb_bid, fb_ask = await self.aster_client.fetch_bbo_prices(self.aster_config.contract_id)
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
) -> None:
    title = f"Cycle {cycle_number} Summary" if cycle_number is not None else "Cycle Summary"
    header = f"\n{title}"
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
        print(line)


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
) -> None:
    header = f"PnL Progress After Cycle {cycle_number}"
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



async def _async_main(args: argparse.Namespace) -> None:
    env_path = Path(args.env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Env file not found: {env_path.resolve()}")

    dotenv.load_dotenv(env_path)

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
            _print_summary(results, cycle_number=cycle_index, logger=executor.logger)

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
            )

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
