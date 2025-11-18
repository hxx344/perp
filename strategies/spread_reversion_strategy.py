#!/usr/bin/env python3
"""Spread reversion strategy with test-mode simulator.

This module implements the spread-widen-entry / spread-narrow-exit logic that was
outlined for the Aster–Lighter pair. It is designed to operate purely in-memory
with deterministic inputs so that we can validate the behaviour in "test mode"
without touching real exchanges.

Key features
------------
- Rolling mean/standard deviation tracking of the mid-price spread between two
  venues.
- Threshold-based entry when the spread widens beyond a configurable z-score
  (or absolute) trigger.
- Exit conditions on z-score reversion, sign flip, or time stop – mirroring the
  production plan while staying completely deterministic.
- Test-mode simulator that walks through historical (or synthetic) data points
  and reports filled trades together with total PnL.

The module exposes a small CLI so that it can be invoked directly:

.. code-block:: bash

    python strategies/spread_reversion_strategy.py --mode test --data sample.csv

When ``--data`` is omitted a synthetic demonstration dataset is generated,
allowing an immediate dry-run. The simulator prints a concise summary of trades
and cumulative PnL so it can be plugged into the wider dashboard tooling later.
"""

from __future__ import annotations

import argparse
import csv
import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import List, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - optional live dependencies
    from strategies.aster_lighter_spread_monitor import SpreadMonitor, SpreadSnapshot

getcontext().prec = 28  # keep plenty of precision for Decimal operations

Direction = str  # simple alias for readability


@dataclass(frozen=True)
class SpreadDataPoint:
    """Single observation of top-of-book quotes for both venues."""

    timestamp: float
    aster_bid: Decimal
    aster_ask: Decimal
    lighter_bid: Decimal
    lighter_ask: Decimal

    @property
    def aster_mid(self) -> Decimal:
        return (self.aster_bid + self.aster_ask) / Decimal("2")

    @property
    def lighter_mid(self) -> Decimal:
        return (self.lighter_bid + self.lighter_ask) / Decimal("2")

    @property
    def spread(self) -> Decimal:
        return self.aster_mid - self.lighter_mid


@dataclass
class StrategyConfig:
    """Parameter bundle for the spread reversion strategy."""

    rolling_window: int = 90
    enter_z: Decimal = Decimal("2.0")
    exit_z: Decimal = Decimal("0.5")
    stop_z: Decimal = Decimal("3.5")
    min_abs_spread: Decimal = Decimal("0")
    quantity: Decimal = Decimal("1")
    max_holding_ticks: int = 900
    aster_fee_rate: Decimal = Decimal("0.0004")
    lighter_fee_rate: Decimal = Decimal("0")

    def validate(self) -> None:
        if self.rolling_window < 10:
            raise ValueError("rolling_window must be >= 10")
        if self.enter_z <= 0:
            raise ValueError("enter_z must be positive")
        if self.exit_z < 0:
            raise ValueError("exit_z must be non-negative")
        if self.stop_z <= self.enter_z:
            raise ValueError("stop_z must exceed enter_z")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")
        if self.max_holding_ticks <= 0:
            raise ValueError("max_holding_ticks must be > 0")
        if self.aster_fee_rate < 0:
            raise ValueError("aster_fee_rate must be >= 0")
        if self.lighter_fee_rate < 0:
            raise ValueError("lighter_fee_rate must be >= 0")


@dataclass
class SpreadPosition:
    """Represents an open hedged position across the two venues."""

    direction: Direction
    quantity: Decimal
    aster_entry_price: Decimal
    lighter_entry_price: Decimal
    entry_spread: Decimal
    entry_timestamp: float
    entry_index: int
    max_z: Decimal = Decimal("0")
    min_z: Decimal = Decimal("0")

    def update_z_extremes(self, z_score: Decimal) -> None:
        if z_score > self.max_z:
            self.max_z = z_score
        if z_score < self.min_z:
            self.min_z = z_score


@dataclass
class TradeRecord:
    """Completed round-trip trade record."""

    direction: Direction
    quantity: Decimal
    entry_timestamp: float
    exit_timestamp: float
    entry_spread: Decimal
    exit_spread: Decimal
    pnl: Decimal
    holding_ticks: int
    max_z: Decimal
    min_z: Decimal


@dataclass
class SimulationResult:
    """Aggregated simulation outcome."""

    trades: List[TradeRecord]
    total_pnl: Decimal
    gross_profit: Decimal
    gross_loss: Decimal

    @property
    def trade_count(self) -> int:
        return len(self.trades)


class RollingStats:
    """Simple rolling statistics helper for Decimal values."""

    def __init__(self, window: int) -> None:
        self._window = window
        self._values: List[Decimal] = []
        self._sum = Decimal("0")
        self._sum_sq = Decimal("0")
        self._index = 0

    def push(self, value: Decimal) -> None:
        if len(self._values) < self._window:
            self._values.append(value)
            self._sum += value
            self._sum_sq += value * value
        else:
            old = self._values[self._index]
            self._values[self._index] = value
            self._sum += value - old
            self._sum_sq += value * value - old * old
        self._index = (self._index + 1) % self._window

    @property
    def ready(self) -> bool:
        return len(self._values) >= self._window

    @property
    def mean(self) -> Optional[Decimal]:
        if not self._values:
            return None
        return self._sum / Decimal(len(self._values))

    @property
    def std(self) -> Optional[Decimal]:
        n = len(self._values)
        if n < 2:
            return None
        mean = self.mean
        if mean is None:
            return None
        variance = (self._sum_sq / Decimal(n)) - mean * mean
        if variance <= 0:
            return Decimal("0")
        try:
            return variance.sqrt()
        except (InvalidOperation, ValueError):
            return Decimal("0")


class SpreadReversionSimulator:
    """Drives the strategy over a sequence of data points."""

    def __init__(self, config: StrategyConfig) -> None:
        config.validate()
        self.config = config
        self.reset()

    def reset(self) -> None:
        self._stats = RollingStats(self.config.rolling_window)
        self._position: Optional[SpreadPosition] = None
        self._trades: List[TradeRecord] = []
        self._gross_profit = Decimal("0")
        self._gross_loss = Decimal("0")
        self._tick_index = 0

    def process(self, points: Sequence[SpreadDataPoint]) -> SimulationResult:
        self.reset()
        last_point: Optional[SpreadDataPoint] = None
        for point in points:
            self.step(point)
            last_point = point

        if self._position is not None and last_point is not None:
            self.force_close(last_point)

        return self.build_result()

    def step(self, point: SpreadDataPoint) -> List[TradeRecord]:
        trades_before = len(self._trades)
        index = self._tick_index
        self._process_point(index, point)
        self._tick_index += 1
        if trades_before < len(self._trades):
            return self._trades[trades_before:]
        return []

    def build_result(self) -> SimulationResult:
        total_pnl = self._gross_profit + self._gross_loss
        return SimulationResult(
            trades=list(self._trades),
            total_pnl=total_pnl,
            gross_profit=self._gross_profit,
            gross_loss=self._gross_loss,
        )

    @property
    def has_open_position(self) -> bool:
        return self._position is not None

    def force_close(self, point: SpreadDataPoint) -> Optional[TradeRecord]:
        if self._position is None:
            return None
        index = max(self._tick_index - 1, self._position.entry_index)
        trades_before = len(self._trades)
        self._close_position(point, index, force=True)
        if len(self._trades) > trades_before:
            return self._trades[-1]
        return None

    def _process_point(self, index: int, point: SpreadDataPoint) -> None:
        spread = point.spread
        self._stats.push(spread)

        mean = self._stats.mean
        std = self._stats.std
        if mean is None or std is None or std == 0:
            return

        z_score = (spread - mean) / std

        if self._position is None:
            self._maybe_open_position(index, point, spread, z_score)
        else:
            self._position.update_z_extremes(z_score)
            self._maybe_close_position(index, point, spread, z_score)

    def _maybe_open_position(
        self,
        index: int,
        point: SpreadDataPoint,
        spread: Decimal,
        z_score: Decimal,
    ) -> None:
        if not self._stats.ready:
            return

        if abs(spread) < self.config.min_abs_spread:
            return

        if abs(z_score) < self.config.enter_z:
            return

        direction: Direction
        if spread >= 0:
            direction = "short_aster_long_lighter"
        else:
            direction = "long_aster_short_lighter"

        self._position = SpreadPosition(
            direction=direction,
            quantity=self.config.quantity,
            aster_entry_price=point.aster_mid,
            lighter_entry_price=point.lighter_mid,
            entry_spread=spread,
            entry_timestamp=point.timestamp,
            entry_index=index,
            max_z=z_score,
            min_z=z_score,
        )

    def _maybe_close_position(
        self,
        index: int,
        point: SpreadDataPoint,
        spread: Decimal,
        z_score: Decimal,
    ) -> None:
        assert self._position is not None
        ticks_held = index - self._position.entry_index

        exit_reason = None
        if abs(z_score) <= self.config.exit_z:
            exit_reason = "reversion"
        elif spread == 0:
            exit_reason = "spread_zero"
        elif (spread - self._position.entry_spread) * self._position.entry_spread < 0:
            # sign flip relative to entry spread direction
            exit_reason = "sign_flip"
        elif abs(z_score) >= self.config.stop_z:
            exit_reason = "stop_z"
        elif ticks_held >= self.config.max_holding_ticks:
            exit_reason = "time_stop"

        if exit_reason:
            self._close_position(point, index, force=False)

    def _close_position(
        self,
        point: SpreadDataPoint,
        index: int,
        force: bool,
    ) -> None:
        position = self._position
        if position is None:
            return

        spread_exit = point.spread
        pnl = self._compute_pnl(position, point)

        trade = TradeRecord(
            direction=position.direction,
            quantity=position.quantity,
            entry_timestamp=position.entry_timestamp,
            exit_timestamp=point.timestamp,
            entry_spread=position.entry_spread,
            exit_spread=spread_exit,
            pnl=pnl,
            holding_ticks=max(index - position.entry_index, 0),
            max_z=position.max_z,
            min_z=position.min_z,
        )
        self._trades.append(trade)

        if pnl >= 0:
            self._gross_profit += pnl
        else:
            self._gross_loss += pnl

        self._position = None

    def _compute_pnl(self, position: SpreadPosition, exit_point: SpreadDataPoint) -> Decimal:
        if position.direction == "short_aster_long_lighter":
            pnl_lighter = (exit_point.lighter_mid - position.lighter_entry_price) * position.quantity
            pnl_aster = (position.aster_entry_price - exit_point.aster_mid) * position.quantity
        elif position.direction == "long_aster_short_lighter":
            pnl_lighter = (position.lighter_entry_price - exit_point.lighter_mid) * position.quantity
            pnl_aster = (exit_point.aster_mid - position.aster_entry_price) * position.quantity
        else:
            raise ValueError(f"Unknown position direction: {position.direction}")
        gross_pnl = pnl_lighter + pnl_aster
        fees = self._compute_fees(position, exit_point)
        return gross_pnl - fees

    def _compute_fees(self, position: SpreadPosition, exit_point: SpreadDataPoint) -> Decimal:
        volume = position.quantity
        fees = Decimal("0")
        if self.config.aster_fee_rate > 0:
            fees += position.aster_entry_price * volume * self.config.aster_fee_rate
            fees += exit_point.aster_mid * volume * self.config.aster_fee_rate
        if self.config.lighter_fee_rate > 0:
            fees += position.lighter_entry_price * volume * self.config.lighter_fee_rate
            fees += exit_point.lighter_mid * volume * self.config.lighter_fee_rate
        return fees


def parse_decimal(value: str, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid decimal for {field_name}: {value}") from exc


def read_csv_points(path: Path) -> List[SpreadDataPoint]:
    points: List[SpreadDataPoint] = []
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"timestamp", "aster_bid", "aster_ask", "lighter_bid", "lighter_ask"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV file {path} missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            points.append(
                SpreadDataPoint(
                    timestamp=float(row["timestamp"]),
                    aster_bid=parse_decimal(row["aster_bid"], field_name="aster_bid"),
                    aster_ask=parse_decimal(row["aster_ask"], field_name="aster_ask"),
                    lighter_bid=parse_decimal(row["lighter_bid"], field_name="lighter_bid"),
                    lighter_ask=parse_decimal(row["lighter_ask"], field_name="lighter_ask"),
                )
            )
    if not points:
        raise ValueError(f"No data points found in {path}")
    return points


def generate_demo_dataset(length: int = 300) -> List[SpreadDataPoint]:
    """Generate a small synthetic dataset for immediate dry-runs."""

    points: List[SpreadDataPoint] = []
    base_aster = Decimal("100")
    base_lighter = Decimal("100")
    spread_shift = Decimal("0")
    for idx in range(length):
        # create gentle oscillations with two forced dislocations
        if idx == length // 3:
            spread_shift += Decimal("3")
        if idx == (2 * length) // 3:
            spread_shift -= Decimal("4")

        offset = Decimal(idx % 25) / Decimal("50")
        aster_mid = base_aster + Decimal("0.2") * offset + spread_shift
        lighter_mid = base_lighter + Decimal("0.15") * offset

        aster_bid = aster_mid - Decimal("0.05")
        aster_ask = aster_mid + Decimal("0.05")
        lighter_bid = lighter_mid - Decimal("0.04")
        lighter_ask = lighter_mid + Decimal("0.04")

        points.append(
            SpreadDataPoint(
                timestamp=float(idx),
                aster_bid=aster_bid,
                aster_ask=aster_ask,
                lighter_bid=lighter_bid,
                lighter_ask=lighter_ask,
            )
        )
    return points


def _snapshot_to_point(snapshot: "SpreadSnapshot") -> Optional[SpreadDataPoint]:
    required = [
        snapshot.aster_bid,
        snapshot.aster_ask,
        snapshot.lighter_bid,
        snapshot.lighter_ask,
    ]
    if any(value is None for value in required):
        return None

    assert snapshot.aster_bid is not None
    assert snapshot.aster_ask is not None
    assert snapshot.lighter_bid is not None
    assert snapshot.lighter_ask is not None

    return SpreadDataPoint(
        timestamp=snapshot.timestamp,
        aster_bid=snapshot.aster_bid,
        aster_ask=snapshot.aster_ask,
        lighter_bid=snapshot.lighter_bid,
        lighter_ask=snapshot.lighter_ask,
    )


def _configure_websocket_logging(debug_websockets: bool) -> None:
    level = logging.DEBUG if debug_websockets else logging.WARNING
    for name in ("websockets", "websockets.client", "websockets.server"):
        logging.getLogger(name).setLevel(level)


def _create_spread_monitor(
    *,
    aster_ticker: str,
    lighter_symbol: str,
    poll_interval: float,
    debug_websockets: bool,
) -> "SpreadMonitor":
    try:
        from strategies.aster_lighter_spread_monitor import SpreadMonitor
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise ModuleNotFoundError(
            "Live mode requires the 'lighter' SDK. Install project extras or disable --live."
        ) from exc

    _configure_websocket_logging(debug_websockets)

    return SpreadMonitor(
        aster_ticker=aster_ticker,
        lighter_symbol=lighter_symbol,
        poll_interval=poll_interval,
        console_table=False,
    )


class LiveSpreadRunner:
    """Run the spread strategy against live market data."""

    def __init__(
        self,
        config: StrategyConfig,
        *,
        aster_ticker: str,
        lighter_symbol: str,
        poll_interval: float,
        run_seconds: Optional[float] = None,
        debug_websockets: bool = False,
    ) -> None:
        self.config = config
        self.aster_ticker = aster_ticker
        self.lighter_symbol = lighter_symbol
        self.poll_interval = max(0.2, float(poll_interval))
        self.run_seconds = run_seconds if run_seconds is None else max(0.0, float(run_seconds))
        self._monitor = _create_spread_monitor(
            aster_ticker=aster_ticker,
            lighter_symbol=lighter_symbol,
            poll_interval=self.poll_interval,
            debug_websockets=debug_websockets,
        )
        self._simulator = SpreadReversionSimulator(config)
        self._last_point: Optional[SpreadDataPoint] = None

    async def run(self) -> SimulationResult:
        await self._monitor.initialize()
        start_time = time.time()
        try:
            while True:
                if self.run_seconds is not None and (time.time() - start_time) >= self.run_seconds:
                    break
                snapshot = await self._monitor._compute_snapshot()
                if snapshot is not None:
                    point = _snapshot_to_point(snapshot)
                    if point is not None:
                        self._last_point = point
                        new_trades = self._simulator.step(point)
                        for trade in new_trades:
                            self._log_trade(trade)
                await asyncio.sleep(self.poll_interval)
        except KeyboardInterrupt:
            print("\nLive mode interrupted by user")
        finally:
            await self._monitor.close()

        if self._last_point is not None and self._simulator.has_open_position:
            forced_trade = self._simulator.force_close(self._last_point)
            if forced_trade is not None:
                self._log_trade(forced_trade, forced=True)

        result = self._simulator.build_result()
        self._print_summary(result)
        return result

    def _log_trade(self, trade: TradeRecord, *, forced: bool = False) -> None:
        flag = "FORCED" if forced else "FILLED"
        print(
            f"[{flag}] dir={trade.direction} qty={trade.quantity} "
            f"spread {trade.entry_spread:.4f}->{trade.exit_spread:.4f} pnl={trade.pnl:.6f}"
        )

    def _print_summary(self, result: SimulationResult) -> None:
        print("=== Live Spread Reversion Summary ===")
        print(f"Trades executed: {result.trade_count}")
        print(f"Gross profit   : {result.gross_profit:.6f}")
        print(f"Gross loss     : {result.gross_loss:.6f}")
        print(f"Net PnL        : {result.total_pnl:.6f}")


def run_cli(args: Optional[Sequence[str]] = None) -> SimulationResult:
    parser = argparse.ArgumentParser(description="Spread reversion test-mode simulator")
    parser.add_argument("--data", type=Path, help="CSV file with timestamp, aster_bid/ask, lighter_bid/ask")
    parser.add_argument("--quantity", type=str, default="1", help="Order size per leg (Decimal)")
    parser.add_argument("--enter-z", type=float, default=2.0, help="Z-score threshold to open a position")
    parser.add_argument("--exit-z", type=float, default=0.5, help="Z-score to close on reversion")
    parser.add_argument("--stop-z", type=float, default=3.5, help="Emergency z-score stop distance")
    parser.add_argument("--window", type=int, default=90, help="Rolling window length for statistics")
    parser.add_argument("--min-abs-spread", type=float, default=0.0, help="Minimum absolute spread to consider")
    parser.add_argument("--max-holding", type=int, default=900, help="Maximum ticks to hold a position")
    parser.add_argument("--demo", action="store_true", help="Ignore --data and run synthetic demo dataset")
    parser.add_argument("--live", action="store_true", help="Run against live exchange data streams")
    parser.add_argument("--aster-ticker", type=str, default="ETH", help="Aster ticker for live mode")
    parser.add_argument("--lighter-symbol", type=str, default="ETH-PERP", help="Lighter symbol for live mode")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Live mode polling interval in seconds")
    parser.add_argument("--run-seconds", type=float, help="Optional duration for live mode before auto-stop")
    parser.add_argument("--aster-fee", type=float, default=0.0004, help="Aster fee rate (e.g. 0.0004 for 4 bps)")
    parser.add_argument("--lighter-fee", type=float, default=0.0, help="Lighter fee rate")
    parser.add_argument(
        "--debug-websockets",
        action="store_true",
        help="Log websocket frames for troubleshooting",
    )

    parsed = parser.parse_args(args=args)

    quantity = parse_decimal(parsed.quantity, field_name="quantity")
    config = StrategyConfig(
        rolling_window=parsed.window,
        enter_z=Decimal(str(parsed.enter_z)),
        exit_z=Decimal(str(parsed.exit_z)),
        stop_z=Decimal(str(parsed.stop_z)),
        min_abs_spread=Decimal(str(parsed.min_abs_spread)),
        quantity=quantity,
        max_holding_ticks=parsed.max_holding,
        aster_fee_rate=Decimal(str(parsed.aster_fee)),
        lighter_fee_rate=Decimal(str(parsed.lighter_fee)),
    )

    if parsed.live:
        runner = LiveSpreadRunner(
            config,
            aster_ticker=parsed.aster_ticker,
            lighter_symbol=parsed.lighter_symbol,
            poll_interval=parsed.poll_interval,
            run_seconds=parsed.run_seconds,
            debug_websockets=parsed.debug_websockets,
        )
        return asyncio.run(runner.run())

    if parsed.demo or parsed.data is None:
        points = generate_demo_dataset()
    else:
        points = read_csv_points(parsed.data)

    simulator = SpreadReversionSimulator(config)
    result = simulator.process(points)

    print("=== Spread Reversion Test Result ===")
    print(f"Trades executed: {result.trade_count}")
    print(f"Gross profit   : {result.gross_profit:.6f}")
    print(f"Gross loss     : {result.gross_loss:.6f}")
    print(f"Net PnL        : {result.total_pnl:.6f}")
    for idx, trade in enumerate(result.trades, start=1):
        print(
            f"#{idx:02d} | dir={trade.direction} | qty={trade.quantity} | "
            f"spread {trade.entry_spread:.4f} -> {trade.exit_spread:.4f} | "
            f"PnL={trade.pnl:.6f}"
        )
    return result


if __name__ == "__main__":  # pragma: no cover - manual invocation
    run_cli()
