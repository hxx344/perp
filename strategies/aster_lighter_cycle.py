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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING, Tuple, cast

import dotenv

from exchanges import ExchangeFactory
from exchanges.base import OrderInfo
from helpers.logger import TradingLogger
from trading_bot import TradingConfig

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from exchanges.aster import AsterClient
    from exchanges.lighter import LighterClient


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

        await self.aster_client.get_contract_attributes()
        await self.lighter_client.get_contract_attributes()
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

    async def shutdown(self) -> None:
        """Ensure both exchange connections are released."""
        tasks = []
        if self.aster_client:
            tasks.append(self.aster_client.disconnect())
        if self.lighter_client:
            tasks.append(self.lighter_client.disconnect())
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
                target_price = await self._calculate_aster_maker_price(direction)
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

        self.logger.log(
            f"{leg_name} | Virtual Aster maker filled {quantity} @ {fill_price} (target {target_price})",
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
        if not contract_id:
            raise RuntimeError("Aster contract id is not initialized")

        deadline = time.time() + self.config.max_wait_seconds
        poll_interval = max(self.config.poll_interval, 0.05)

        while True:
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
                        f"{leg_name} | Lighter flagged price as accidental at slippage {current_slippage}%. Tightening and retrying...",
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
                    f"{leg_name} | Attempt {attempt} failed to place Lighter order: {error_message}",
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

            try:
                fill_info = await self._wait_for_lighter_fill(str(order_result.order_id), leg_name)
            except TimeoutError as exc:
                last_error = exc
                self.logger.log(
                    f"{leg_name} | Attempt {attempt} timed out waiting for Lighter fill. Canceling and retrying...",
                    "WARNING",
                )
                cancel_success = await self._attempt_cancel_lighter(str(order_result.order_id), leg_name)
                if not cancel_success:
                    raise RuntimeError(
                        f"{leg_name} | Cancel failed after timeout; aborting to avoid duplicate exposure"
                    )
                if attempt >= max_attempts:
                    raise
                if current_slippage <= Decimal("0"):
                    current_slippage = Decimal("0.01")
                else:
                    current_slippage = min(
                        (current_slippage * Decimal("2")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP),
                        Decimal("5"),
                    )
                await asyncio.sleep(self.config.retry_delay_seconds)
                continue

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

    async def _wait_for_lighter_fill(self, client_order_id: str, leg_name: str) -> OrderInfo:
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

        cancel_success = await self._attempt_cancel_lighter(client_order_id, leg_name)
        if not cancel_success:
            raise RuntimeError(
                f"{leg_name} | Cancel failed after timeout; aborting to avoid duplicate exposure"
            )
        raise TimeoutError(f"{leg_name} | Lighter order {client_order_id} not filled within timeout")

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

    async def _attempt_cancel_lighter(self, client_order_id: str, leg_name: str) -> bool:
        if not self.lighter_client:
            return False

        current_order = getattr(self.lighter_client, "current_order", None)
        order_index = None
        if current_order:
            order_index = getattr(current_order, "order_id", None)

        if order_index is None:
            self.logger.log(
                f"{leg_name} | Unable to cancel Lighter order {client_order_id}: missing order index",
                "WARNING",
            )
            return False

        try:
            result = await self.lighter_client.cancel_order(str(order_index))
            if not result.success:
                self.logger.log(
                    f"{leg_name} | Failed to cancel Lighter order {order_index}: {result.error_message}",
                    "ERROR",
                )
                return False
            return True
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.log(
                f"{leg_name} | Exception canceling Lighter order {order_index}: {exc}",
                "ERROR",
            )
            return False

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

        price = depth_price if depth_price is not None else None

        if price is None or price <= 0:
            fallback_bid, fallback_ask = await self.aster_client.fetch_bbo_prices(
                self.aster_config.contract_id
            )
            best_bid = fallback_bid if fallback_bid > 0 else best_bid
            best_ask = fallback_ask if fallback_ask > 0 else best_ask

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
            raise ValueError("Unable to determine Aster maker price from market data")

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

        return price

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

        return _round_to_tick(price, tick)

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
            fill_info = await self._wait_for_lighter_fill(str(order_result.order_id), "EMERGENCY_FLATTEN")
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


def _print_summary(results: List[LegResult], cycle_number: Optional[int] = None) -> None:
    title = f"Cycle {cycle_number} Summary" if cycle_number is not None else "Cycle Summary"
    header = f"\n{title}"
    print(header)
    print("=" * len(header))
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

        print(" | ".join(parts))


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
            _print_summary(results, cycle_number=cycle_index)

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
