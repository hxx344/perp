"""Minimal spot-mode hedging loop for Aster/Lighter.

This script is meant to be a readable reference rather than a full-featured runner.
It reuses the production ``HedgingCycleExecutor`` but forces the Lighter leg to spot
mode, captures the initial inventory automatically, and prints concise per-cycle
summaries.  Copy the file, tweak the defaults or environment variables below, and
run it inside the same virtualenv that already has your credentials configured.

Environment knobs (all optional):
    SPOT_ENV_FILE          -> override .env file path (default: ./.env)
    SPOT_ASTER_TICKER      -> defaults to "ETH"
    SPOT_LIGHTER_TICKER    -> defaults to "ETH-PERP"
    SPOT_DIRECTION         -> "buy" or "sell" (default: buy)
    SPOT_QUANTITY          -> per-leg size, Decimal string (default: 0.5)
    SPOT_SLIPPAGE_PCT      -> Lighter taker slippage percentage (default: 0.08)
    SPOT_MAX_CYCLES        -> run N cycles then stop (default: 1, set 0 for infinite)
    SPOT_CYCLE_DELAY       -> extra seconds between cycles (default: 1.0)
    SPOT_VIRTUAL_MAKER     -> "1" to simulate Aster maker legs locally

Usage (PowerShell):
    cd d:/project8/perp-dex-tools
    $env:SPOT_MAX_CYCLES=3
    D:/project8/.venv/Scripts/python.exe examples/lighter_spot_cycle_example.py
"""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal, InvalidOperation
from typing import Iterable

import dotenv

from strategies.aster_lighter_cycle import CycleConfig, HedgingCycleExecutor, LegResult


def _decimal_from_env(var_name: str, fallback: Decimal) -> Decimal:
    raw = os.getenv(var_name)
    if not raw:
        return fallback
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return fallback


def _int_from_env(var_name: str, fallback: int) -> int:
    raw = os.getenv(var_name)
    if not raw:
        return fallback
    try:
        return int(raw)
    except ValueError:
        return fallback


def _bool_from_env(var_name: str, fallback: bool = False) -> bool:
    raw = os.getenv(var_name)
    if not raw:
        return fallback
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _print_cycle_summary(cycle_idx: int, results: Iterable[LegResult]) -> None:
    print(f"\nCycle #{cycle_idx} summary")
    print("-----------------------")
    for leg in results:
        print(
            f"{leg.name:>4s} | {leg.exchange.upper():6s} | {leg.side.upper():4s} | "
            f"qty={leg.quantity} | fill={leg.price} | latency={leg.latency_seconds:.3f}s"
        )


async def main() -> None:
    env_file = os.getenv("SPOT_ENV_FILE", ".env")
    if os.path.exists(env_file):
        dotenv.load_dotenv(env_file)

    base_quantity = _decimal_from_env("SPOT_QUANTITY", Decimal("0.5"))
    aster_ticker = (os.getenv("SPOT_ASTER_TICKER", "ETH") or "ETH").upper()
    lighter_ticker = os.getenv("SPOT_LIGHTER_TICKER", "ETH-PERP") or "ETH-PERP"
    direction = os.getenv("SPOT_DIRECTION", "buy").lower()
    if direction not in {"buy", "sell"}:
        direction = "buy"

    config = CycleConfig(
        aster_ticker=aster_ticker,
        lighter_ticker=lighter_ticker,
        quantity=base_quantity,
        aster_quantity=base_quantity,
        lighter_quantity=base_quantity,
        direction=direction,
        take_profit_pct=Decimal("0"),
        slippage_pct=_decimal_from_env("SPOT_SLIPPAGE_PCT", Decimal("0.08")),
        max_wait_seconds=5.0,
        lighter_max_wait_seconds=120.0,
        poll_interval=0.1,
        max_retries=200,
        retry_delay_seconds=5.0,
        max_cycles=0,
        delay_between_cycles=float(os.getenv("SPOT_CYCLE_DELAY", "1.0")),
        virtual_aster_maker=_bool_from_env("SPOT_VIRTUAL_MAKER"),
        lighter_market_type="spot",
        preserve_initial_position=True,
        lighter_spot_market_id=os.getenv("SPOT_LIGHTER_MARKET_ID"),
    )

    executor = HedgingCycleExecutor(config)
    await executor.setup()
    cycle_limit = _int_from_env("SPOT_MAX_CYCLES", 1)
    print(
        "Spot hedging loop ready -> Aster %s / Lighter %s (direction=%s, qty=%s)"
        % (aster_ticker, lighter_ticker, direction, base_quantity)
    )

    cycle_idx = 0
    try:
        while True:
            cycle_idx += 1
            results = await executor.execute_cycle()
            _print_cycle_summary(cycle_idx, results)

            if cycle_limit > 0 and cycle_idx >= cycle_limit:
                break

            if config.delay_between_cycles > 0:
                await asyncio.sleep(config.delay_between_cycles)
    finally:
        await executor.ensure_lighter_flat()
        await executor.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
