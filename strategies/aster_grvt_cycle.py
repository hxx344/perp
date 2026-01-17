#!/usr/bin/env python3
"""Aster–GRVT hedging cycle executor.

This module is based on `strategies/aster_lighter_cycle.py` and adapts the
four-leg cycle to:

    - Leg 1: Aster **virtual maker** entry (signal/price discovery only).
    - Leg 2: GRVT **market** order to take the hedge exposure.
    - Leg 3: Aster **virtual maker** reverse/exit (signal/price discovery only).
    - Leg 4: GRVT **market** order to flatten.

Key idea
--------
Aster side never places real orders; it acts as a virtual maker to generate
price signals for GRVT to execute with market orders.

The implementation intentionally keeps the same high-level structure and
runtime knobs as the aster-lighter executor (retry loops, tick rounding,
depth-level pricing, optional coordinator reporting hook).

Note
----
This file only relies on existing exchange adapters in this repo:
- `exchanges.aster` for public market data (virtual pricing)
- `exchanges.grvt` for trading (market orders are executed using GRVT adapter)

If you want GRVT to use true market orders, we add a small helper to the GRVT
adapter in this commit.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, cast

import dotenv

from exchanges import ExchangeFactory
from exchanges.base import OrderInfo
from helpers.logger import TradingLogger
from trading_bot import TradingConfig

try:  # pragma: no cover
    from .hedge_reporter import HedgeMetricsReporter
except ImportError:  # pragma: no cover
    from hedge_reporter import HedgeMetricsReporter

from exchanges.aster import AsterMarketDataWebSocket

DEFAULT_ASTER_MAKER_DEPTH_LEVEL = 10
MIN_CYCLE_INTERVAL_SECONDS = 5.0

if TYPE_CHECKING:  # pragma: no cover
    from exchanges.aster import AsterClient
    from exchanges.grvt import GrvtClient


def _decimal_type(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid decimal value: {value}") from exc


@dataclass
class CycleConfig:
    aster_ticker: str
    grvt_ticker: str
    quantity: Decimal
    grvt_quantity: Decimal
    direction: str
    slippage_pct: Decimal
    max_wait_seconds: float
    poll_interval: float
    max_retries: int
    retry_delay_seconds: float
    max_cycles: int
    delay_between_cycles: float

    # virtual maker knobs (copied from aster-lighter)
    virtual_aster_maker: bool = True
    virtual_aster_price_source: str = "aster"  # currently only `aster` (public data)
    aster_maker_depth_level: int = DEFAULT_ASTER_MAKER_DEPTH_LEVEL
    aster_leg1_depth_level: Optional[int] = None
    aster_leg3_depth_level: Optional[int] = None

    # coordinator reporting (optional)
    coordinator_url: Optional[str] = None
    coordinator_agent_id: Optional[str] = None
    coordinator_pause_poll_seconds: float = 5.0
    coordinator_username: Optional[str] = None
    coordinator_password: Optional[str] = None

    # logging
    log_to_console: bool = True


@dataclass
class LegResult:
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
    pass


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):  # pragma: no cover - defensive
        return None


def _extract_price_quantity(leg: LegResult) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    price = _to_decimal(getattr(leg, "price", None))
    quantity = _to_decimal(getattr(leg, "quantity", None))
    if price is None or quantity is None:
        return None, None
    try:
        quantity = abs(quantity)
    except Exception:  # pragma: no cover
        return None, None
    return price, quantity


def _calculate_pair_metrics(leg_a: LegResult, leg_b: LegResult) -> Tuple[Decimal, Decimal]:
    """Return (pnl, volume) for a matched pair.

    We match on min(|qty_a|, |qty_b|) and compute:
    - PnL: (sell_price - buy_price) * matched_qty when opposite sides
    - Volume: (sell_price + buy_price) * matched_qty

    If both sides are the same (shouldn't happen in expected cycles), we fallback
    to matched cash-flow.
    """

    price_a, qty_a = _extract_price_quantity(leg_a)
    price_b, qty_b = _extract_price_quantity(leg_b)
    if price_a is None or price_b is None or qty_a is None or qty_b is None:
        return Decimal("0"), Decimal("0")

    matched_qty = min(qty_a, qty_b)
    if matched_qty <= 0:
        return Decimal("0"), Decimal("0")

    side_a = (leg_a.side or "").lower()
    side_b = (leg_b.side or "").lower()
    valid_sides = {"buy", "sell"}
    if side_a not in valid_sides or side_b not in valid_sides:
        return Decimal("0"), Decimal("0")

    if side_a == side_b:
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
    """Cycle-level metrics.

    Pairing rule mirrors aster-lighter:
    - (leg1, leg3) is one round-trip
    - (leg2, leg4) is the other round-trip

    Note: for this strategy, leg1/leg3 are *virtual* and do not represent
    realized PnL. We therefore skip any pair that involves VIRTUAL legs to
    avoid distorting PnL/volume.
    """

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


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return value
    return value.quantize(tick, rounding=ROUND_HALF_UP)


def _compute_cycle_pause_seconds(
    cycle_start_time: float,
    configured_delay: float,
    *,
    enforce_min_interval: bool = True,
) -> float:
    elapsed = max(0.0, time.time() - cycle_start_time)
    if enforce_min_interval:
        remaining_for_min = max(0.0, MIN_CYCLE_INTERVAL_SECONDS - elapsed)
        return float(max(configured_delay, remaining_for_min))
    return float(max(0.0, configured_delay))


class _AsterPublicDataClient:
    """Minimal Aster public depth feed helper.

    We keep this small and compatible with the websocket helper used elsewhere.
    """

    _WS_URL = "wss://fstream.asterdex.com"

    def __init__(self, ticker: str, logger: TradingLogger, *, depth_levels: int = 10) -> None:
        self.ticker = ticker.upper().strip()
        self.logger = logger
        self.depth_levels = max(5, min(int(depth_levels or 10), 50))
        self.contract_symbol = f"{self.ticker}USDT"  # Aster futures naming
        self._ws: Optional[AsterMarketDataWebSocket] = None

    async def start(self) -> None:
        symbol = self.contract_symbol.lower()
        self._ws = AsterMarketDataWebSocket(
            symbol=symbol,
            base_ws_url=self._WS_URL,
            logger=self.logger,
            depth_levels=self.depth_levels,
        )
        await self._ws.start()
        await self._ws.wait_until_ready(timeout=5.0)

    async def stop(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.stop()
            except Exception:
                pass
            self._ws = None

    async def fetch_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        if self._ws is None:
            raise RuntimeError("Aster public WS not started")
        bids, asks = await self._ws.get_depth(limit=1)
        best_bid = bids[0][0] if bids else Decimal("0")
        best_ask = asks[0][0] if asks else Decimal("0")
        return best_bid, best_ask

    async def get_depth_level_price(self, direction: str, level: int) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        if self._ws is None:
            raise RuntimeError("Aster public WS not started")
        lvl = max(1, min(int(level or 1), self.depth_levels))
        bids, asks = await self._ws.get_depth(limit=max(lvl, 1))
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        if direction.lower() == "buy":
            depth_price = bids[lvl - 1][0] if bids and len(bids) >= lvl else (bids[-1][0] if bids else None)
        else:
            depth_price = asks[lvl - 1][0] if asks and len(asks) >= lvl else (asks[-1][0] if asks else None)
        return depth_price, best_bid, best_ask


class HedgingCycleExecutor:
    """Coordinates the four-leg hedging cycle between Aster (virtual) and GRVT (market)."""

    @staticmethod
    def _normalize_agent_identifier(value: Optional[str]) -> str:
        text = (str(value).strip() if value is not None else "")
        if not text:
            try:
                text = socket.gethostname().strip()
            except Exception:
                text = ""
        return (text or "default")[:120]

    def __init__(self, config: CycleConfig):
        self.config = config
        ticker_label = f"{config.aster_ticker}_{config.grvt_ticker}".replace("/", "-")
        self.logger = TradingLogger(
            exchange="hedge",
            ticker=ticker_label,
            log_to_console=bool(getattr(config, "log_to_console", True)),
        )

        self._coordinator_agent_id = self._normalize_agent_identifier(getattr(config, "coordinator_agent_id", None))
        self._metrics_reporter: Optional[HedgeMetricsReporter] = None
        self._coordinator_paused = False
        self._pause_poll_seconds = max(1.0, float(getattr(config, "coordinator_pause_poll_seconds", 5.0) or 5.0))
        self._run_started_at = time.time()

        # Depth levels: leg-specific override or fallback to maker depth
        base_depth = int(getattr(config, "aster_maker_depth_level", DEFAULT_ASTER_MAKER_DEPTH_LEVEL) or DEFAULT_ASTER_MAKER_DEPTH_LEVEL)
        self._aster_maker_depth_level = max(1, min(base_depth, 500))
        self._aster_leg1_depth_level = int(getattr(config, "aster_leg1_depth_level", None) or self._aster_maker_depth_level)
        self._aster_leg3_depth_level = int(getattr(config, "aster_leg3_depth_level", None) or self._aster_maker_depth_level)

        # Trading configs
        self.grvt_config = TradingConfig(
            ticker=config.grvt_ticker.upper(),
            contract_id="",
            quantity=config.grvt_quantity,
            take_profit=Decimal("0"),
            tick_size=Decimal(0),
            direction=config.direction,
            max_orders=1,
            wait_time=0,
            exchange="grvt",
            grid_step=Decimal("-100"),
            stop_price=Decimal("-1"),
            pause_price=Decimal("-1"),
            boost_mode=False,
        )

        # We still keep an Aster TradingConfig for compatibility with ExchangeFactory contract attributes.
        self.aster_config = TradingConfig(
            ticker=config.aster_ticker.upper(),
            contract_id="",
            quantity=config.quantity,
            take_profit=Decimal("0"),
            tick_size=Decimal(0),
            direction=config.direction,
            max_orders=1,
            wait_time=0,
            exchange="aster",
            grid_step=Decimal("-100"),
            stop_price=Decimal("-1"),
            pause_price=Decimal("-1"),
            boost_mode=False,
            maker_depth_level=self._aster_leg1_depth_level,
        )

        self.grvt_client: Optional["GrvtClient"] = None
        self._aster_public: Optional[_AsterPublicDataClient] = None

    async def setup(self) -> None:
        """Create clients, connect GRVT, and start Aster public market data."""

        grvt_client = ExchangeFactory.create_exchange("grvt", self.grvt_config)  # type: ignore[arg-type]
        self.grvt_client = cast("GrvtClient", grvt_client)
        await self.grvt_client.connect()
        await self.grvt_client.get_contract_attributes()

        # Start Aster market data (virtual maker)
        self._aster_public = _AsterPublicDataClient(
            ticker=self.config.aster_ticker,
            logger=self.logger,
            depth_levels=max(self._aster_leg1_depth_level, self._aster_leg3_depth_level, 10),
        )
        await self._aster_public.start()

        # coordinator (optional)
        if getattr(self.config, "coordinator_url", None):
            self._metrics_reporter = HedgeMetricsReporter(
                base_url=str(self.config.coordinator_url),
                auth_username=getattr(self.config, "coordinator_username", None),
                auth_password=getattr(self.config, "coordinator_password", None),
            )

        aster_symbol = getattr(self._aster_public, "contract_symbol", None)
        self.logger.log(
            (
                "Setup complete | "
                f"Aster virtual ticker={self.config.aster_ticker} symbol={aster_symbol} "
                f"(leg1_depth={self._aster_leg1_depth_level}, leg3_depth={self._aster_leg3_depth_level}) | "
                f"GRVT ticker={self.config.grvt_ticker} contract_id={self.grvt_config.contract_id}"
            ),
            "INFO",
        )

    async def shutdown(self) -> None:
        tasks = []
        if self._aster_public is not None:
            tasks.append(self._aster_public.stop())
        if self.grvt_client is not None:
            tasks.append(self.grvt_client.disconnect())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.log("Shutdown complete", "INFO")

    async def _refresh_pause_state(self) -> bool:
        if not self._metrics_reporter:
            self._coordinator_paused = False
            return False
        try:
            snapshot = await self._metrics_reporter.fetch_control(agent_id=self._coordinator_agent_id)
        except Exception:
            return self._coordinator_paused

        paused = False
        if isinstance(snapshot, dict):
            if isinstance(snapshot.get("paused"), bool):
                paused = bool(snapshot.get("paused"))
            elif isinstance(snapshot.get("agent"), dict):
                paused = bool(snapshot["agent"].get("paused", False))
        self._coordinator_paused = paused
        return paused

    async def wait_for_resume(self, context: str) -> None:
        if not self._metrics_reporter:
            self._coordinator_paused = False
            return

        while True:
            paused = await self._refresh_pause_state()
            if not paused:
                return
            self.logger.log(f"Coordinator pause active ({context}); waiting...", "WARNING")
            await asyncio.sleep(self._pause_poll_seconds)

    async def report_metrics(self, *, total_cycles: int, cumulative_pnl: Decimal, cumulative_volume: Decimal) -> None:
        if not self._metrics_reporter:
            return
        try:
            runtime_seconds = max(0.0, time.time() - self._run_started_at)
            position = Decimal("0")
            if self.grvt_client and not self._coordinator_paused:
                try:
                    position = await self.grvt_client.get_account_positions()
                except Exception:
                    position = Decimal("0")

            await self._metrics_reporter.report(
                position=position,
                total_cycles=total_cycles,
                cumulative_pnl=cumulative_pnl,
                cumulative_volume=cumulative_volume,
                agent_id=self._coordinator_agent_id,
                available_balance=None,
                total_account_value=None,
                instrument=f"ASTER(v) {self.config.aster_ticker} / GRVT {self.config.grvt_ticker}",
                depths={
                    "maker": int(self._aster_maker_depth_level),
                    "leg1": int(self._aster_leg1_depth_level),
                    "leg3": int(self._aster_leg3_depth_level),
                },
                runtime_seconds=runtime_seconds,
            )
        except Exception as exc:
            self.logger.log(f"Failed to report metrics: {exc}", "WARNING")

    async def execute_cycle(self) -> List[LegResult]:
        if not self.grvt_client:
            raise RuntimeError("GRVT client not ready; call setup()")
        if not self._aster_public:
            raise RuntimeError("Aster public data not ready; call setup()")

        results: List[LegResult] = []
        entry_direction = (self.config.direction or "").lower()
        if entry_direction not in {"buy", "sell"}:
            raise ValueError(f"Invalid direction: {self.config.direction}")

        # Leg1: virtual maker entry signal
        leg1 = await self._simulate_virtual_aster_leg(
            leg_name="LEG1",
            direction=entry_direction,
            quantity=self.config.quantity,
            depth_level=self._aster_leg1_depth_level,
        )
        results.append(leg1)

        # Leg2: GRVT market
        leg2_direction = "sell" if entry_direction == "buy" else "buy"
        leg2 = await self._execute_grvt_market(
            leg_name="LEG2",
            direction=leg2_direction,
            reference_price=leg1.price,
        )
        results.append(leg2)

        # Leg3: virtual maker exit signal
        reverse_direction = "sell" if entry_direction == "buy" else "buy"
        leg3 = await self._simulate_virtual_aster_leg(
            leg_name="LEG3",
            direction=reverse_direction,
            quantity=self.config.quantity,
            depth_level=self._aster_leg3_depth_level,
        )
        results.append(leg3)

        # Leg4: GRVT market to flatten
        leg4 = await self._execute_grvt_market(
            leg_name="LEG4",
            direction=entry_direction,
            reference_price=leg3.price,
        )
        results.append(leg4)

        self.logger.log("Hedging cycle finished", "INFO")
        return results

    async def _simulate_virtual_aster_leg(self, *, leg_name: str, direction: str, quantity: Decimal, depth_level: int) -> LegResult:
        if not self._aster_public:
            raise RuntimeError("Aster public data not started")

        overall_start = time.time()
        attempt = 0
        last_error: Optional[str] = None

        while True:
            attempt += 1
            if attempt > 1:
                await asyncio.sleep(self.config.retry_delay_seconds)

            try:
                target_price = await self._calculate_virtual_maker_price(direction, depth_level=depth_level)
            except Exception as exc:
                last_error = str(exc)
                if self.config.max_retries > 0 and attempt >= self.config.max_retries:
                    raise RuntimeError(f"{leg_name} virtual pricing failed: {last_error}")
                continue

            try:
                fill_price, fill_ts = await self._wait_for_virtual_fill(direction, target_price)
                break
            except TimeoutError as exc:
                last_error = str(exc)
                if self.config.max_retries > 0 and attempt >= self.config.max_retries:
                    raise
                continue

        latency = max(fill_ts - overall_start, 0.0)
        order_id = f"virtual-{leg_name}-{int(fill_ts * 1000)}"
        self.logger.log(f"{leg_name} | Aster virtual maker filled {quantity} @ {fill_price} (target={target_price})", "INFO")

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

    async def _calculate_virtual_maker_price(self, direction: str, *, depth_level: int) -> Decimal:
        if not self._aster_public:
            raise RuntimeError("Aster public data not started")

        depth_price, best_bid, best_ask = await self._aster_public.get_depth_level_price(direction, depth_level)
        if depth_price is None or depth_price <= 0:
            # fallback to bbo
            bid, ask = await self._aster_public.fetch_bbo_prices()
            best_bid = bid
            best_ask = ask
            if direction == "buy":
                depth_price = best_bid
            else:
                depth_price = best_ask

        if depth_price is None or depth_price <= 0:
            raise RuntimeError("Unable to determine Aster virtual maker price")

        # Aster side acts as maker, so we want prices consistent with direction
        # buy  -> use bid-side depth (more conservative)
        # sell -> use ask-side depth
        return depth_price

    async def _wait_for_virtual_fill(self, direction: str, target_price: Decimal) -> Tuple[Decimal, float]:
        if not self._aster_public:
            raise RuntimeError("Aster public data not started")

        deadline = time.time() + float(self.config.max_wait_seconds)
        poll_interval = max(float(self.config.poll_interval), 0.05)

        while True:
            best_bid, best_ask = await self._aster_public.fetch_bbo_prices()
            now = time.time()

            if direction == "buy":
                # virtual buy: wait until ask <= target
                if best_ask > 0 and best_ask <= target_price:
                    return best_ask, now
            else:
                # virtual sell: wait until bid >= target
                if best_bid > 0 and best_bid >= target_price:
                    return best_bid, now

            if now >= deadline:
                break

            await asyncio.sleep(poll_interval)

        raise TimeoutError(f"Virtual fill not reached for target={target_price}")

    async def _execute_grvt_market(self, *, leg_name: str, direction: str, reference_price: Decimal) -> LegResult:
        if not self.grvt_client:
            raise RuntimeError("GRVT client not connected")

        overall_start = time.time()
        attempt = 0
        last_error: Optional[str] = None

        while True:
            attempt += 1
            if attempt > 1:
                await asyncio.sleep(self.config.retry_delay_seconds)

            try:
                # Note: we execute market order; reference_price is for logging only.
                order_result = await self.grvt_client.place_market_order(
                    self.grvt_config.contract_id,
                    self.config.grvt_quantity,
                    direction,
                )
                if not order_result.success or not order_result.order_id:
                    raise RuntimeError(order_result.error_message or "market order failed")

                fill_info = await self._wait_for_grvt_fill(str(order_result.order_id), leg_name)
                break
            except Exception as exc:
                last_error = str(exc)
                self.logger.log(f"{leg_name} | GRVT market attempt {attempt} failed: {last_error}", "ERROR")
                if self.config.max_retries > 0 and attempt >= self.config.max_retries:
                    raise
                continue

        latency = time.time() - overall_start
        self.logger.log(
            f"{leg_name} | GRVT market filled {fill_info.filled_size} @ {fill_info.price} (ref={reference_price})",
            "INFO",
        )

        return LegResult(
            name=leg_name,
            exchange="grvt",
            side=direction,
            quantity=fill_info.filled_size,
            price=fill_info.price,
            order_id=fill_info.order_id,
            status=fill_info.status,
            latency_seconds=latency,
            requested_price=None,
            reference_price=reference_price,
        )

    async def _wait_for_grvt_fill(self, order_id: str, leg_name: str) -> OrderInfo:
        if not self.grvt_client:
            raise RuntimeError("GRVT client is not connected")

        deadline = time.time() + float(self.config.max_wait_seconds)
        last_status: Optional[str] = None

        while time.time() < deadline:
            info = await self.grvt_client.get_order_info(order_id=order_id)
            if info:
                last_status = info.status
                if info.status == "FILLED":
                    return info
                if info.status in {"CANCELED", "REJECTED", "EXPIRED"}:
                    raise RuntimeError(f"{leg_name} | GRVT order ended with status {info.status}")
            await asyncio.sleep(float(self.config.poll_interval))

        # Best-effort cancel to avoid stuck orders
        try:
            await self.grvt_client.cancel_order(order_id)
        except Exception:
            pass

        final_info = await self.grvt_client.get_order_info(order_id=order_id)
        if final_info and final_info.status == "FILLED":
            return final_info

        raise TimeoutError(f"{leg_name} | GRVT order {order_id} not filled within timeout (last={last_status})")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aster–GRVT hedging cycle executor")
    parser.add_argument("--aster-ticker", required=True)
    parser.add_argument(
        "--grvt-ticker",
        required=True,
        help="GRVT base ticker, e.g. ETH or BTC (no suffix)",
    )
    parser.add_argument("--quantity", type=_decimal_type, required=True, help="Aster virtual quantity")
    parser.add_argument("--grvt-quantity", type=_decimal_type, required=True)
    parser.add_argument("--direction", choices=["buy", "sell"], required=True)
    parser.add_argument("--slippage-pct", type=_decimal_type, default=Decimal("0.01"))
    parser.add_argument("--max-wait-seconds", type=float, default=10.0)
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=float, default=0.5)
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--delay-between-cycles", type=float, default=5.0)
    parser.add_argument("--aster-maker-depth-level", type=int, default=DEFAULT_ASTER_MAKER_DEPTH_LEVEL)
    parser.add_argument("--aster-leg1-depth-level", type=int, default=None)
    parser.add_argument("--aster-leg3-depth-level", type=int, default=None)
    parser.add_argument("--coordinator-url", default=None)
    parser.add_argument("--coordinator-agent-id", default=None)
    parser.add_argument("--coordinator-username", default=None)
    parser.add_argument("--coordinator-password", default=None)
    parser.add_argument("--coordinator-pause-poll-seconds", type=float, default=5.0)
    parser.add_argument("--no-console", action="store_true")
    return parser


async def _run_from_args(args: argparse.Namespace) -> None:
    config = CycleConfig(
        aster_ticker=str(args.aster_ticker),
        grvt_ticker=str(args.grvt_ticker),
        quantity=Decimal(str(args.quantity)),
        grvt_quantity=Decimal(str(args.grvt_quantity)),
        direction=str(args.direction),
        slippage_pct=Decimal(str(args.slippage_pct)),
        max_wait_seconds=float(args.max_wait_seconds),
        poll_interval=float(args.poll_interval),
        max_retries=int(args.max_retries),
        retry_delay_seconds=float(args.retry_delay_seconds),
        max_cycles=int(args.max_cycles),
        delay_between_cycles=float(args.delay_between_cycles),
        aster_maker_depth_level=int(args.aster_maker_depth_level),
        aster_leg1_depth_level=args.aster_leg1_depth_level,
        aster_leg3_depth_level=args.aster_leg3_depth_level,
        coordinator_url=args.coordinator_url,
        coordinator_agent_id=args.coordinator_agent_id,
        coordinator_username=args.coordinator_username,
        coordinator_password=args.coordinator_password,
        coordinator_pause_poll_seconds=float(args.coordinator_pause_poll_seconds),
        log_to_console=not bool(args.no_console),
    )

    executor = HedgingCycleExecutor(config)
    await executor.setup()

    total_cycles = 0
    cumulative_pnl = Decimal("0")
    cumulative_volume = Decimal("0")

    try:
        while config.max_cycles <= 0 or total_cycles < config.max_cycles:
            await executor.wait_for_resume("cycle")
            cycle_started_at = time.time()
            results = await executor.execute_cycle()
            total_cycles += 1

            cycle_pnl = _calculate_cycle_pnl(results)
            cycle_volume = _calculate_cycle_volume(results)
            cumulative_pnl += cycle_pnl
            cumulative_volume += cycle_volume

            cycle_duration_seconds = max(0.0, time.time() - cycle_started_at)
            executor.logger.log(
                (
                    f"Cycle {total_cycles} metrics | "
                    f"cycle_pnl={cycle_pnl:.8f} cum_pnl={cumulative_pnl:.8f} | "
                    f"cycle_volume={cycle_volume:.4f} cum_volume={cumulative_volume:.4f} | "
                    f"duration={cycle_duration_seconds:.2f}s"
                ),
                "INFO",
            )

            await executor.report_metrics(
                total_cycles=total_cycles,
                cumulative_pnl=cumulative_pnl,
                cumulative_volume=cumulative_volume,
            )

            pause = _compute_cycle_pause_seconds(
                cycle_started_at,
                config.delay_between_cycles,
                enforce_min_interval=True,
            )
            await asyncio.sleep(pause)
    finally:
        await executor.shutdown()


def main() -> None:
    dotenv.load_dotenv()
    parser = _build_arg_parser()
    args = parser.parse_args()
    asyncio.run(_run_from_args(args))


if __name__ == "__main__":
    main()
