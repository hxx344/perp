"""Educational paper-only Aster execution state machine.

This module demonstrates spread gates, fee/wear accounting, inventory caps,
and reverse-direction state transitions. It has no API credentials and its
execution adapter intentionally raises if asked to place a live order.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import aiohttp

from strategies.aster_cost_monitor import BBO, DEFAULT_FEE_RATE, DEFAULT_REST_URL

LOGGER = logging.getLogger("strategies.aster_volume_paper_strategy")


@dataclass(slots=True)
class PaperSettings:
    symbol: str = "XAUUSD1"
    max_spread_bps: Decimal = Decimal("8")
    cycle_notional: Decimal = Decimal("100")
    max_position_quantity: Decimal = Decimal("0.01")
    inventory_flip_threshold: Decimal = Decimal("0.005")
    fee_rate: Decimal = DEFAULT_FEE_RATE


@dataclass(slots=True)
class PaperState:
    position: Decimal = Decimal("0")
    direction: str = "BUY"
    cycles: int = 0
    volume: Decimal = Decimal("0")
    fees: Decimal = Decimal("0")
    spread_wear: Decimal = Decimal("0")
    skipped: int = 0


class LiveExecutionAdapter:
    """Deliberately non-functional placeholder for studying API boundaries."""

    async def market_order(self, *_args, **_kwargs):
        raise RuntimeError("paper-only module: live order submission is disabled")


class PaperStrategy:
    def __init__(self, settings: PaperSettings):
        self.settings = settings
        self.state = PaperState()
        self.adapter = LiveExecutionAdapter()
        self._last_bbo: Optional[BBO] = None

    def should_trade(self, bbo: BBO) -> bool:
        return bbo.spread_bps <= self.settings.max_spread_bps and bbo.mid > 0

    def _quantity(self, bbo: BBO) -> Decimal:
        return min(self.settings.cycle_notional / bbo.mid, self.settings.max_position_quantity)

    def paper_cycle(self, bbo: BBO) -> dict:
        self._last_bbo = bbo
        if not self.should_trade(bbo):
            self.state.skipped += 1
            return {"status": "skipped", "reason": "spread_above_threshold", "spread_bps": bbo.spread_bps}
        quantity = self._quantity(bbo)
        if quantity <= 0:
            self.state.skipped += 1
            return {"status": "skipped", "reason": "zero_quantity"}

        # A BUY opens at ask and closes at bid. A SELL opens at bid and closes
        # at ask. This intentionally models adverse crossing, not a fictitious
        # maker fill at the midpoint.
        if self.state.direction == "BUY":
            open_price, close_price = bbo.ask, bbo.bid
            signed = quantity
        else:
            open_price, close_price = bbo.bid, bbo.ask
            signed = -quantity
        notional = quantity * ((open_price + close_price) / Decimal("2"))
        fees = notional * self.settings.fee_rate * Decimal("2")
        wear = quantity * abs(open_price - close_price) + fees
        self.state.position += signed
        peak_position = self.state.position
        # The paper cycle closes exactly what it opened. In a live adapter this
        # is where partial fills and a fresh position read must be handled.
        self.state.position -= signed
        self.state.cycles += 1
        self.state.volume += notional * Decimal("2")
        self.state.fees += fees
        self.state.spread_wear += wear
        if abs(peak_position) >= self.settings.inventory_flip_threshold:
            self.state.direction = "SELL" if self.state.direction == "BUY" else "BUY"
        return {
            "status": "paper_completed",
            "direction": "BUY" if signed > 0 else "SELL",
            "quantity": quantity,
            "open_price": open_price,
            "close_price": close_price,
            "notional": notional,
            "fees": fees,
            "wear": wear,
            "position_after": self.state.position,
            "next_direction": self.state.direction,
        }

    def summary(self) -> dict:
        return {
            "mode": "paper_only",
            "symbol": self.settings.symbol,
            "cycles": self.state.cycles,
            "volume": self.state.volume,
            "fees": self.state.fees,
            "spread_wear": self.state.spread_wear,
            "position": self.state.position,
            "next_direction": self.state.direction,
            "skipped": self.state.skipped,
        }


async def fetch_bbo(session: aiohttp.ClientSession, rest_url: str, symbol: str) -> BBO:
    async with session.get(
        f"{rest_url.rstrip('/')}/fapi/v1/ticker/bookTicker",
        params={"symbol": symbol},
        timeout=aiohttp.ClientTimeout(total=5),
    ) as response:
        payload = await response.json()
        if response.status != 200:
            raise RuntimeError(f"Aster public API HTTP {response.status}: {payload}")
        bid = Decimal(str(payload["bidPrice"]))
        ask = Decimal(str(payload["askPrice"]))
        if bid <= 0 or ask <= bid:
            raise RuntimeError("invalid Aster BBO")
        return BBO(time.time(), bid, ask)


async def run(settings: PaperSettings, rest_url: str, cycles: int, poll_seconds: float) -> None:
    strategy = PaperStrategy(settings)
    async with aiohttp.ClientSession() as session:
        for _ in range(cycles):
            bbo = await fetch_bbo(session, rest_url, settings.symbol)
            LOGGER.info("paper cycle: %s", strategy.paper_cycle(bbo))
            await asyncio.sleep(poll_seconds)
    LOGGER.info("paper summary: %s", strategy.summary())


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper-only Aster execution state machine")
    parser.add_argument("--symbol", default="XAUUSD1")
    parser.add_argument("--max-spread-bps", type=Decimal, default=Decimal("8"))
    parser.add_argument("--cycle-notional", type=Decimal, default=Decimal("100"))
    parser.add_argument("--max-position-quantity", type=Decimal, default=Decimal("0.01"))
    parser.add_argument("--cycles", type=int, default=10)
    parser.add_argument("--poll-seconds", type=float, default=5)
    parser.add_argument("--rest-url", default=DEFAULT_REST_URL)
    args = parser.parse_args()
    settings = PaperSettings(args.symbol, args.max_spread_bps, args.cycle_notional, args.max_position_quantity)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(run(settings, args.rest_url, args.cycles, args.poll_seconds))


if __name__ == "__main__":
    main()
