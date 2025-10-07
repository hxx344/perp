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
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING, cast

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
    direction: str
    take_profit_pct: Decimal  # retained for compatibility; reverse leg now ignores this value
    slippage_pct: Decimal
    max_wait_seconds: float
    poll_interval: float
    max_retries: int
    retry_delay_seconds: float
    max_cycles: int
    delay_between_cycles: float


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


class HedgingCycleExecutor:
    """Coordinates the four-leg hedging cycle between Aster and Lighter."""

    def __init__(self, config: CycleConfig):
        self.config = config
        ticker_label = f"{config.aster_ticker}_{config.lighter_ticker}".replace("/", "-")
        self.logger = TradingLogger(exchange="hedge", ticker=ticker_label, log_to_console=True)

        self.aster_config = TradingConfig(
            ticker=config.aster_ticker.upper(),
            contract_id="",
            quantity=config.quantity,
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
            quantity=config.quantity,
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

        self.logger.log("Hedging cycle completed successfully", "INFO")
        return results

    async def _execute_aster_maker(self, leg_name: str, direction: str) -> LegResult:
        if not self.aster_client:
            raise RuntimeError("Aster client is not connected")

        overall_start = time.time()
        attempt = 0
        skip_retry_delay = False
        while True:
            attempt += 1
            if attempt > 1 and not skip_retry_delay:
                await asyncio.sleep(self.config.retry_delay_seconds)
            skip_retry_delay = False

            order_result = await self.aster_client.place_open_order(
                self.aster_config.contract_id, self.config.quantity, direction
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

        overall_start = time.time()
        attempt = 0
        skip_retry_delay = False
        while True:
            attempt += 1
            if attempt > 1 and not skip_retry_delay:
                await asyncio.sleep(self.config.retry_delay_seconds)
            skip_retry_delay = False

            best_bid, best_ask = await self.aster_client.fetch_bbo_prices(self.aster_config.contract_id)
            target_price = self._calculate_aster_maker_price(direction, best_bid, best_ask)
            order_result = await self.aster_client.place_close_order(
                self.aster_config.contract_id, self.config.quantity, target_price, direction
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

        for attempt in range(1, max_attempts + 1):
            target_price = self._calculate_taker_price_from_reference(
                direction=direction,
                reference_price=reference_price,
                slippage_pct=current_slippage,
            )

            start = time.time()
            order_result = await self.lighter_client.place_limit_order(
                self.lighter_config.contract_id,
                self.config.quantity,
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
                await self._attempt_cancel_lighter(str(order_result.order_id), leg_name)
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

        deadline = time.time() + self.config.max_wait_seconds
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

        await self._attempt_cancel_lighter(client_order_id, leg_name)
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

    async def _attempt_cancel_lighter(self, client_order_id: str, leg_name: str) -> None:
        if not self.lighter_client:
            return

        current_order = getattr(self.lighter_client, "current_order", None)
        order_index = None
        if current_order:
            order_index = getattr(current_order, "order_id", None)

        if order_index is None:
            self.logger.log(
                f"{leg_name} | Unable to cancel Lighter order {client_order_id}: missing order index",
                "WARNING",
            )
            return

        try:
            result = await self.lighter_client.cancel_order(str(order_index))
            if not result.success:
                self.logger.log(
                    f"{leg_name} | Failed to cancel Lighter order {order_index}: {result.error_message}",
                    "ERROR",
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.log(
                f"{leg_name} | Exception canceling Lighter order {order_index}: {exc}",
                "ERROR",
            )

    def _calculate_aster_maker_price(
        self, direction: str, best_bid: Decimal, best_ask: Decimal
    ) -> Decimal:
        tick = self.aster_config.tick_size if self.aster_config.tick_size > 0 else Decimal("0")

        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("Invalid Aster order book data for maker pricing")

        if direction == "sell":
            price = best_bid + tick if tick > 0 else best_ask
        else:
            price = best_ask - tick if tick > 0 else best_bid

        if price <= 0 and tick > 0:
            price = tick

        return _round_to_tick(price, tick)

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
        "--poll-interval",
        type=float,
        default=0.2,
        help="Polling interval when waiting for order status updates",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=100,
        help="Maximum number of retries for Aster maker orders before aborting",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=5.0,
        help="Delay in seconds before retrying an Aster maker order",
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
            f"qty={leg.quantity}",
            f"fill={leg.price}",
            f"status={leg.status}",
            f"latency={leg.latency_seconds:.3f}s",
        ]

        if leg.requested_price is not None:
            parts.insert(3, f"limit={leg.requested_price}")
        if leg.reference_price is not None:
            parts.insert(3, f"ref={leg.reference_price}")

        print(" | ".join(parts))


async def _async_main(args: argparse.Namespace) -> None:
    env_path = Path(args.env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Env file not found: {env_path.resolve()}")

    dotenv.load_dotenv(env_path)

    config = CycleConfig(
        aster_ticker=args.aster_ticker,
        lighter_ticker=args.lighter_ticker,
        quantity=args.quantity,
        direction=args.direction,
        take_profit_pct=args.take_profit,
        slippage_pct=args.slippage,
        max_wait_seconds=args.max_wait,
        poll_interval=args.poll_interval,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay,
        max_cycles=max(0, args.cycles),
        delay_between_cycles=max(0.0, args.cycle_delay),
    )

    executor = HedgingCycleExecutor(config)
    await executor.setup()
    cycle_index = 0
    try:
        while True:
            cycle_index += 1
            executor.logger.log(f"Starting hedging cycle #{cycle_index}", "INFO")
            results = await executor.execute_cycle()
            executor.logger.log(f"Completed hedging cycle #{cycle_index}", "INFO")
            _print_summary(results, cycle_number=cycle_index)

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
