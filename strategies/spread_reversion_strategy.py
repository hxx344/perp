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
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Sequence, TYPE_CHECKING

import aiohttp
import requests

if TYPE_CHECKING:  # pragma: no cover - optional live dependencies
    from strategies.aster_lighter_spread_monitor import SpreadMonitor, SpreadSnapshot

getcontext().prec = 28  # keep plenty of precision for Decimal operations

Direction = str  # simple alias for readability

EVENT_HISTORY_LIMIT = 200
PAYLOAD_EVENT_LIMIT = 60
PAYLOAD_TRADE_LIMIT = 30


class StrategyMetricsFormatter:
    """Utility helpers to serialize simulator state for the coordinator."""

    @staticmethod
    def decimal_to_str(value: Optional[Decimal]) -> Optional[str]:
        if value is None:
            return None
        try:
            return format(value, "f")
        except Exception:
            return str(value)

    @classmethod
    def trade_to_dict(cls, trade: "TradeRecord") -> Dict[str, Any]:
        return {
            "direction": trade.direction,
            "quantity": cls.decimal_to_str(trade.quantity),
            "entry_timestamp": trade.entry_timestamp,
            "exit_timestamp": trade.exit_timestamp,
            "entry_spread": cls.decimal_to_str(trade.entry_spread),
            "exit_spread": cls.decimal_to_str(trade.exit_spread),
            "notional": cls.decimal_to_str(trade.notional),
            "pnl": cls.decimal_to_str(trade.pnl),
            "holding_ticks": trade.holding_ticks,
            "max_z": cls.decimal_to_str(trade.max_z),
            "min_z": cls.decimal_to_str(trade.min_z),
        }

    @classmethod
    def event_to_dict(cls, event: "DecisionEvent") -> Dict[str, Any]:
        return {
            "timestamp": event.timestamp,
            "action": event.action,
            "direction": event.direction,
            "spread": cls.decimal_to_str(event.spread),
            "z_score": cls.decimal_to_str(event.z_score),
            "quantity": cls.decimal_to_str(event.quantity),
            "reason": event.reason,
            "pnl": cls.decimal_to_str(event.pnl),
            "forced": event.forced,
        }

    @classmethod
    def config_to_payload(cls, config: "StrategyConfig", *, poll_interval: Optional[float]) -> Dict[str, Any]:
        return {
            "rolling_window": config.rolling_window,
            "enter_z": cls.decimal_to_str(config.enter_z),
            "exit_z": cls.decimal_to_str(config.exit_z),
            "stop_z": cls.decimal_to_str(config.stop_z),
            "min_abs_spread": cls.decimal_to_str(config.min_abs_spread),
            "quantity": cls.decimal_to_str(config.quantity),
            "max_holding_ticks": config.max_holding_ticks,
            "aster_fee_rate": cls.decimal_to_str(config.aster_fee_rate),
            "lighter_fee_rate": cls.decimal_to_str(config.lighter_fee_rate),
            "poll_interval": poll_interval,
            "lighter_only": config.lighter_only,
            "min_expected_profit": cls.decimal_to_str(config.min_expected_profit),
        }

    @classmethod
    def summary_to_payload(cls, summary: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "trade_count": int(summary.get("trade_count", 0)),
            "gross_profit": cls.decimal_to_str(summary.get("gross_profit")),
            "gross_loss": cls.decimal_to_str(summary.get("gross_loss")),
            "total_pnl": cls.decimal_to_str(summary.get("total_pnl")),
            "total_volume": cls.decimal_to_str(summary.get("total_volume")),
            "pnl_over_volume": cls.decimal_to_str(summary.get("pnl_over_volume")),
        }

    @classmethod
    def position_to_payload(cls, snapshot: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if snapshot is None:
            return None
        return {
            "direction": snapshot.get("direction"),
            "quantity": cls.decimal_to_str(snapshot.get("quantity")),
            "aster_entry_price": cls.decimal_to_str(snapshot.get("aster_entry_price")),
            "lighter_entry_price": cls.decimal_to_str(snapshot.get("lighter_entry_price")),
            "entry_spread": cls.decimal_to_str(snapshot.get("entry_spread")),
            "entry_timestamp": snapshot.get("entry_timestamp"),
            "ticks_held": snapshot.get("ticks_held"),
            "entry_index": snapshot.get("entry_index"),
            "max_z": cls.decimal_to_str(snapshot.get("max_z")),
            "min_z": cls.decimal_to_str(snapshot.get("min_z")),
        }

    @classmethod
    def signal_to_payload(
        cls,
        latest_signal: Dict[str, Optional[Decimal]],
        *,
        timestamp: Optional[float],
    ) -> Dict[str, Any]:
        return {
            "spread": cls.decimal_to_str(latest_signal.get("spread")),
            "z_score": cls.decimal_to_str(latest_signal.get("z_score")),
            "timestamp": timestamp,
        }

    @classmethod
    def point_to_payload(cls, point: "SpreadDataPoint") -> Dict[str, Any]:
        return {
            "timestamp": point.timestamp,
            "aster_bid": cls.decimal_to_str(point.aster_bid),
            "aster_ask": cls.decimal_to_str(point.aster_ask),
            "lighter_bid": cls.decimal_to_str(point.lighter_bid),
            "lighter_ask": cls.decimal_to_str(point.lighter_ask),
            "aster_mid": cls.decimal_to_str(point.aster_mid),
            "lighter_mid": cls.decimal_to_str(point.lighter_mid),
            "spread": cls.decimal_to_str(point.spread),
        }

    @classmethod
    def build_payload(
        cls,
        *,
        agent_id: str,
        instrument: str,
        config: "StrategyConfig",
        poll_interval: Optional[float],
        summary: Dict[str, Any],
        open_position: Optional[Dict[str, Any]],
        latest_signal: Dict[str, Optional[Decimal]],
        signal_timestamp: Optional[float],
        events: Sequence[Dict[str, Any]],
        trades: Sequence[Dict[str, Any]],
        latest_point: Optional["SpreadDataPoint"],
        updated_at: Optional[float] = None,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {
            "updated_at": updated_at or time.time(),
            "config": cls.config_to_payload(config, poll_interval=poll_interval),
            "summary": cls.summary_to_payload(summary),
            "open_position": cls.position_to_payload(open_position),
            "latest_signal": cls.signal_to_payload(latest_signal, timestamp=signal_timestamp),
            "recent_events": list(events),
            "recent_trades": list(trades),
        }
        if latest_point is not None:
            metrics["latest_point"] = cls.point_to_payload(latest_point)
        if mode:
            metrics["mode"] = mode
        metrics["structure"] = "lighter_only" if config.lighter_only else "multi_venue"

        return {
            "agent_id": agent_id,
            "instrument": instrument,
            "strategy_metrics": metrics,
        }


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


@dataclass(frozen=True)
class DecisionEvent:
    """Discrete decision emitted by the simulator."""

    timestamp: float
    action: str
    direction: Optional[Direction]
    spread: Decimal
    z_score: Optional[Decimal]
    quantity: Optional[Decimal]
    reason: Optional[str] = None
    pnl: Optional[Decimal] = None
    forced: bool = False


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
    lighter_only: bool = False
    min_expected_profit: Decimal = Decimal("0")

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
        if self.min_expected_profit < 0:
            raise ValueError("min_expected_profit must be >= 0")


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
    notional: Decimal
    pnl: Decimal
    holding_ticks: int
    max_z: Decimal
    min_z: Decimal


@dataclass
class SimulationResult:
    """Aggregated simulation outcome."""

    trades: List[TradeRecord]
    total_pnl: Decimal
    total_volume: Decimal
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

    def __init__(self, config: StrategyConfig, *, event_history_limit: int = EVENT_HISTORY_LIMIT) -> None:
        config.validate()
        self.config = config
        self._event_history_limit = max(1, int(event_history_limit))
        self._event_log: Deque[DecisionEvent] = deque(maxlen=self._event_history_limit)
        self._last_z_score: Optional[Decimal] = None
        self._last_spread: Optional[Decimal] = None
        self._last_timestamp: Optional[float] = None
        self.reset()

    def reset(self) -> None:
        self._stats = RollingStats(self.config.rolling_window)
        self._position: Optional[SpreadPosition] = None
        self._trades: List[TradeRecord] = []
        self._gross_profit = Decimal("0")
        self._gross_loss = Decimal("0")
        self._total_volume = Decimal("0")
        self._tick_index = 0
        self._event_log = deque(maxlen=self._event_history_limit)
        self._last_z_score = None
        self._last_spread = None
        self._last_timestamp = None

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
            total_volume=self._total_volume,
            gross_profit=self._gross_profit,
            gross_loss=self._gross_loss,
        )

    def consume_events(self) -> List[DecisionEvent]:
        events = list(self._event_log)
        self._event_log.clear()
        return events

    def get_aggregate_metrics(self) -> Dict[str, Any]:
        total_pnl = self._gross_profit + self._gross_loss
        pnl_over_volume: Optional[Decimal]
        if self._total_volume != 0:
            pnl_over_volume = total_pnl / self._total_volume
        else:
            pnl_over_volume = None
        return {
            "trade_count": len(self._trades),
            "gross_profit": self._gross_profit,
            "gross_loss": self._gross_loss,
            "total_pnl": total_pnl,
            "total_volume": self._total_volume,
            "pnl_over_volume": pnl_over_volume,
        }

    def get_open_position_snapshot(self) -> Optional[Dict[str, Any]]:
        position = self._position
        if position is None:
            return None
        ticks_held = max(self._tick_index - position.entry_index, 0)
        return {
            "direction": position.direction,
            "quantity": position.quantity,
            "aster_entry_price": position.aster_entry_price,
            "lighter_entry_price": position.lighter_entry_price,
            "entry_spread": position.entry_spread,
            "entry_timestamp": position.entry_timestamp,
            "ticks_held": ticks_held,
            "entry_index": position.entry_index,
            "max_z": position.max_z,
            "min_z": position.min_z,
        }

    def get_latest_signal(self) -> Dict[str, Optional[Decimal]]:
        return {
            "spread": self._last_spread,
            "z_score": self._last_z_score,
        }

    def get_recent_trades(self, limit: int = 20) -> List[TradeRecord]:
        if limit <= 0:
            return []
        return self._trades[-limit:]

    def get_last_timestamp(self) -> Optional[float]:
        return self._last_timestamp

    @property
    def has_open_position(self) -> bool:
        return self._position is not None

    def force_close(self, point: SpreadDataPoint) -> Optional[TradeRecord]:
        if self._position is None:
            return None
        index = max(self._tick_index - 1, self._position.entry_index)
        trades_before = len(self._trades)
        self._close_position(point, index, force=True, reason="force_close")
        if len(self._trades) > trades_before:
            return self._trades[-1]
        return None

    def _process_point(self, index: int, point: SpreadDataPoint) -> None:
        spread = point.spread
        self._stats.push(spread)
        self._last_timestamp = point.timestamp
        self._last_spread = spread

        mean = self._stats.mean
        std = self._stats.std
        self._last_z_score = None
        if mean is None or std is None or std == 0:
            return

        z_score = (spread - mean) / std
        self._last_z_score = z_score

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
        if self.config.lighter_only:
            direction = "lighter_long" if spread >= 0 else "lighter_short"
        else:
            if spread >= 0:
                direction = "short_aster_long_lighter"
            else:
                direction = "long_aster_short_lighter"

        expected_profit = self._estimate_expected_profit(point, spread, direction)
        if expected_profit < self.config.min_expected_profit:
            return

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
        self._record_event(
            action="enter",
            point=point,
            spread=spread,
            z_score=z_score,
            direction=self._entry_order_direction(direction),
            quantity=self.config.quantity,
            reason="enter_z_trigger",
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
            self._close_position(point, index, force=False, reason=exit_reason)

    def _close_position(
        self,
        point: SpreadDataPoint,
        index: int,
        *,
        force: bool,
        reason: Optional[str] = None,
    ) -> None:
        position = self._position
        if position is None:
            return

        spread_exit = point.spread
        pnl = self._compute_pnl(position, point)
        trade_volume = self._compute_trade_volume(position, point)

        trade = TradeRecord(
            direction=position.direction,
            quantity=position.quantity,
            entry_timestamp=position.entry_timestamp,
            exit_timestamp=point.timestamp,
            entry_spread=position.entry_spread,
            exit_spread=spread_exit,
            notional=trade_volume,
            pnl=pnl,
            holding_ticks=max(index - position.entry_index, 0),
            max_z=position.max_z,
            min_z=position.min_z,
        )
        self._trades.append(trade)
        self._total_volume += trade_volume

        if pnl >= 0:
            self._gross_profit += pnl
        else:
            self._gross_loss += pnl

        self._position = None
        self._record_event(
            action="exit",
            point=point,
            spread=spread_exit,
            z_score=self._last_z_score,
            direction=self._exit_order_direction(position.direction),
            quantity=position.quantity,
            reason=reason or ("force_close" if force else None),
            pnl=pnl,
            forced=force,
        )

    def _record_event(
        self,
        *,
        action: str,
        point: SpreadDataPoint,
        spread: Decimal,
        z_score: Optional[Decimal],
        direction: Optional[Direction],
        quantity: Optional[Decimal],
        reason: Optional[str] = None,
        pnl: Optional[Decimal] = None,
        forced: bool = False,
    ) -> None:
        self._event_log.append(
            DecisionEvent(
                timestamp=point.timestamp,
                action=action,
                direction=direction,
                spread=spread,
                z_score=z_score,
                quantity=quantity,
                reason=reason,
                pnl=pnl,
                forced=forced,
            )
        )

    def _compute_pnl(self, position: SpreadPosition, exit_point: SpreadDataPoint) -> Decimal:
        if position.direction == "short_aster_long_lighter":
            pnl_lighter = (exit_point.lighter_mid - position.lighter_entry_price) * position.quantity
            pnl_aster = (position.aster_entry_price - exit_point.aster_mid) * position.quantity
        elif position.direction == "long_aster_short_lighter":
            pnl_lighter = (position.lighter_entry_price - exit_point.lighter_mid) * position.quantity
            pnl_aster = (exit_point.aster_mid - position.aster_entry_price) * position.quantity
        elif position.direction == "lighter_long":
            pnl_lighter = (exit_point.lighter_mid - position.lighter_entry_price) * position.quantity
            pnl_aster = Decimal("0")
        elif position.direction == "lighter_short":
            pnl_lighter = (position.lighter_entry_price - exit_point.lighter_mid) * position.quantity
            pnl_aster = Decimal("0")
        else:
            raise ValueError(f"Unknown position direction: {position.direction}")
        gross_pnl = pnl_lighter + pnl_aster
        fees = self._compute_fees(position, exit_point)
        return gross_pnl - fees

    def _compute_trade_volume(self, position: SpreadPosition, exit_point: SpreadDataPoint) -> Decimal:
        quantity = abs(position.quantity)
        volume = Decimal("0")
        if position.direction in ("short_aster_long_lighter", "long_aster_short_lighter"):
            volume += (position.aster_entry_price + exit_point.aster_mid) * quantity
            volume += (position.lighter_entry_price + exit_point.lighter_mid) * quantity
        elif position.direction in ("lighter_long", "lighter_short"):
            volume += (position.lighter_entry_price + exit_point.lighter_mid) * quantity
        else:
            raise ValueError(f"Unknown position direction for volume: {position.direction}")
        return volume

    @staticmethod
    def _entry_order_direction(position_direction: Direction) -> Direction:
        mapping = {
            "short_aster_long_lighter": "sell_aster / buy_lighter",
            "long_aster_short_lighter": "buy_aster / sell_lighter",
            "lighter_long": "buy_lighter",
            "lighter_short": "sell_lighter",
        }
        if position_direction not in mapping:
            raise ValueError(f"Unknown position direction for entry event: {position_direction}")
        return mapping[position_direction]

    @staticmethod
    def _exit_order_direction(position_direction: Direction) -> Direction:
        mapping = {
            "short_aster_long_lighter": "buy_aster / sell_lighter",
            "long_aster_short_lighter": "sell_aster / buy_lighter",
            "lighter_long": "sell_lighter",
            "lighter_short": "buy_lighter",
        }
        if position_direction not in mapping:
            raise ValueError(f"Unknown position direction for exit event: {position_direction}")
        return mapping[position_direction]

    def _compute_fees(self, position: SpreadPosition, exit_point: SpreadDataPoint) -> Decimal:
        volume = position.quantity
        fees = Decimal("0")
        direction = position.direction
        if direction in ("short_aster_long_lighter", "long_aster_short_lighter"):
            if self.config.aster_fee_rate > 0:
                fees += position.aster_entry_price * volume * self.config.aster_fee_rate
                fees += exit_point.aster_mid * volume * self.config.aster_fee_rate
            if self.config.lighter_fee_rate > 0:
                fees += position.lighter_entry_price * volume * self.config.lighter_fee_rate
                fees += exit_point.lighter_mid * volume * self.config.lighter_fee_rate
        elif direction in ("lighter_long", "lighter_short"):
            if self.config.lighter_fee_rate > 0:
                fees += position.lighter_entry_price * volume * self.config.lighter_fee_rate
                fees += exit_point.lighter_mid * volume * self.config.lighter_fee_rate
        else:
            raise ValueError(f"Unknown position direction for fees: {direction}")
        return fees

    def _estimate_expected_profit(
        self,
        point: SpreadDataPoint,
        spread: Decimal,
        direction: Direction,
    ) -> Decimal:
        quantity = abs(self.config.quantity)
        gross_edge = abs(spread) * quantity
        fee_penalty = self._estimate_round_trip_fees(point, direction, quantity)
        return gross_edge - fee_penalty

    def _estimate_round_trip_fees(
        self,
        point: SpreadDataPoint,
        direction: Direction,
        quantity: Decimal,
    ) -> Decimal:
        fees = Decimal("0")
        if direction in ("short_aster_long_lighter", "long_aster_short_lighter"):
            if self.config.aster_fee_rate > 0:
                fees += point.aster_mid * quantity * self.config.aster_fee_rate * Decimal("2")
            if self.config.lighter_fee_rate > 0:
                fees += point.lighter_mid * quantity * self.config.lighter_fee_rate * Decimal("2")
        elif direction in ("lighter_long", "lighter_short"):
            if self.config.lighter_fee_rate > 0:
                fees += point.lighter_mid * quantity * self.config.lighter_fee_rate * Decimal("2")
        else:
            raise ValueError(f"Unknown position direction for fee estimation: {direction}")
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
        coordinator_url: Optional[str] = None,
        coordinator_agent_id: Optional[str] = None,
        coordinator_push_interval: Optional[float] = None,
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
        self._coordinator_url = (coordinator_url or "").strip() or None
        if self._coordinator_url is not None:
            self._coordinator_url = self._coordinator_url.rstrip("/")
        default_agent = f"{self.aster_ticker}-{self.lighter_symbol}-spread"
        self._coordinator_agent_id = (coordinator_agent_id or default_agent).strip() or default_agent
        self._coordinator_session: Optional[aiohttp.ClientSession] = None
        self._coordinator_push_interval = (
            max(self.poll_interval, 2.0)
            if coordinator_push_interval is None
            else max(0.5, float(coordinator_push_interval))
        )
        self._recent_events: Deque[Dict[str, Any]] = deque(maxlen=EVENT_HISTORY_LIMIT)
        self._recent_trades: Deque[Dict[str, Any]] = deque(maxlen=PAYLOAD_TRADE_LIMIT * 3)
        self._last_push_ts = 0.0
        self._logger = logging.getLogger(__name__)

    async def run(self) -> SimulationResult:
        await self._monitor.initialize()
        if self._coordinator_url is not None:
            timeout = aiohttp.ClientTimeout(total=10)
            self._coordinator_session = aiohttp.ClientSession(timeout=timeout)
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
                        events = self._simulator.consume_events()
                        for event in events:
                            self._recent_events.append(StrategyMetricsFormatter.event_to_dict(event))
                        for trade in new_trades:
                            trade_dict = StrategyMetricsFormatter.trade_to_dict(trade)
                            self._recent_trades.append(trade_dict)
                            self._log_trade(trade)
                        await self._maybe_push_metrics(
                            new_events=bool(events),
                            new_trades=bool(new_trades),
                        )
                await asyncio.sleep(self.poll_interval)
        except KeyboardInterrupt:
            print("\nLive mode interrupted by user")
        finally:
            await self._monitor.close()

        if self._last_point is not None and self._simulator.has_open_position:
            forced_trade = self._simulator.force_close(self._last_point)
            if forced_trade is not None:
                trade_dict = StrategyMetricsFormatter.trade_to_dict(forced_trade)
                self._recent_trades.append(trade_dict)
                self._log_trade(forced_trade, forced=True)
            forced_events = self._simulator.consume_events()
            for event in forced_events:
                self._recent_events.append(StrategyMetricsFormatter.event_to_dict(event))
            await self._maybe_push_metrics(
                new_events=bool(forced_events),
                new_trades=forced_trade is not None,
                force=True,
            )
        else:
            await self._maybe_push_metrics(new_events=False, new_trades=False, force=True)

        if self._coordinator_session is not None:
            try:
                await self._coordinator_session.close()
            except Exception:
                pass
            self._coordinator_session = None

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

    async def _maybe_push_metrics(self, *, new_events: bool, new_trades: bool, force: bool = False) -> None:
        if self._coordinator_url is None or self._coordinator_session is None:
            return

        now = time.time()
        if not force and not new_events and not new_trades and (now - self._last_push_ts) < self._coordinator_push_interval:
            return

        payload = self._build_metrics_payload()
        if payload is None:
            return

        endpoint = f"{self._coordinator_url}/update"
        try:
            async with self._coordinator_session.post(endpoint, json=payload) as response:
                if response.status >= 400:
                    text = await response.text()
                    self._logger.warning("Coordinator update failed (%s): %s", response.status, text)
                    return
        except Exception as exc:
            self._logger.warning("Coordinator update error: %s", exc)
            return

        self._last_push_ts = now

    def _build_metrics_payload(self) -> Optional[Dict[str, Any]]:
        summary = self._simulator.get_aggregate_metrics()
        open_position = self._simulator.get_open_position_snapshot()
        latest_signal = self._simulator.get_latest_signal()

        events = list(self._recent_events)[-PAYLOAD_EVENT_LIMIT:]
        trades = list(self._recent_trades)[-PAYLOAD_TRADE_LIMIT:]
        signal_timestamp = (
            self._last_point.timestamp
            if self._last_point is not None
            else self._simulator.get_last_timestamp()
        )

        return StrategyMetricsFormatter.build_payload(
            agent_id=self._coordinator_agent_id,
            instrument=f"{self.aster_ticker}/{self.lighter_symbol}",
            config=self.config,
            poll_interval=self.poll_interval,
            summary=summary,
            open_position=open_position,
            latest_signal=latest_signal,
            signal_timestamp=signal_timestamp,
            events=events,
            trades=trades,
            latest_point=self._last_point,
        )


def _post_metrics_to_coordinator(
    *,
    coordinator_url: str,
    payload: Dict[str, Any],
    agent_id: str,
) -> None:
    endpoint = f"{coordinator_url.rstrip('/')}/update"
    try:
        response = requests.post(endpoint, json=payload, timeout=10)
    except Exception as exc:  # pragma: no cover - network path
        print(f"[coordinator] Upload error: {exc}")
        return

    if response.status_code >= 400:
        try:
            body = response.text
        except Exception:
            body = "<no body>"
        print(f"[coordinator] Failed to upload metrics ({response.status_code}): {body}")
    else:
        print(f"[coordinator] Metrics posted to {endpoint} as agent '{agent_id}'.")


def run_cli(args: Optional[Sequence[str]] = None) -> SimulationResult:
    parser = argparse.ArgumentParser(description="Spread reversion test-mode simulator")
    parser.add_argument("--data", type=Path, help="CSV file with timestamp, aster_bid/ask, lighter_bid/ask")
    parser.add_argument("--quantity", type=str, default="1", help="Order size per leg (Decimal)")
    parser.add_argument("--enter-z", type=float, default=2.0, help="Z-score threshold to open a position")
    parser.add_argument("--exit-z", type=float, default=0.5, help="Z-score to close on reversion")
    parser.add_argument("--stop-z", type=float, default=3.5, help="Emergency z-score stop distance")
    parser.add_argument("--window", type=int, default=90, help="Rolling window length for statistics")
    parser.add_argument("--min-abs-spread", type=float, default=0.0, help="Minimum absolute spread to consider")
    parser.add_argument(
        "--min-expected-profit",
        type=float,
        default=0.0,
        help="Minimum expected round-trip profit (in quote currency) before opening a trade",
    )
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
        "--lighter-only",
        action="store_true",
        help="Simulate single-venue execution on Lighter (no Aster leg)",
    )
    parser.add_argument(
        "--debug-websockets",
        action="store_true",
        help="Log websocket frames for troubleshooting",
    )
    parser.add_argument(
        "--coordinator-url",
        type=str,
        help="Optional coordinator endpoint to stream live metrics (e.g. http://localhost:8899)",
    )
    parser.add_argument(
        "--coordinator-agent",
        type=str,
        help="Identifier used when reporting to the coordinator dashboard",
    )
    parser.add_argument(
        "--coordinator-push-interval",
        type=float,
        help="Minimum seconds between coordinator updates (defaults to poll interval)",
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
        lighter_only=parsed.lighter_only,
        min_expected_profit=Decimal(str(parsed.min_expected_profit)),
    )

    if parsed.live:
        runner = LiveSpreadRunner(
            config,
            aster_ticker=parsed.aster_ticker,
            lighter_symbol=parsed.lighter_symbol,
            poll_interval=parsed.poll_interval,
            run_seconds=parsed.run_seconds,
            debug_websockets=parsed.debug_websockets,
            coordinator_url=parsed.coordinator_url,
            coordinator_agent_id=parsed.coordinator_agent,
            coordinator_push_interval=parsed.coordinator_push_interval,
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
    print(f"Total volume   : {result.total_volume:.6f}")
    if result.total_volume != 0:
        ratio_percent = (result.total_pnl / result.total_volume) * Decimal("100")
        print(f"PnL / volume   : {ratio_percent:.2f}%")
    else:
        print("PnL / volume   : --")
    for idx, trade in enumerate(result.trades, start=1):
        print(
            f"#{idx:02d} | dir={trade.direction} | qty={trade.quantity} | "
            f"spread {trade.entry_spread:.4f} -> {trade.exit_spread:.4f} | "
            f"PnL={trade.pnl:.6f}"
        )

    coordinator_url = (parsed.coordinator_url or "").strip()
    if coordinator_url:
        coordinator_url = coordinator_url.rstrip("/")
        default_agent = f"{parsed.aster_ticker}-{parsed.lighter_symbol}-spread"
        agent_id = (parsed.coordinator_agent or default_agent).strip() or default_agent
        summary = simulator.get_aggregate_metrics()
        open_position = simulator.get_open_position_snapshot()
        latest_signal = simulator.get_latest_signal()
        last_point = points[-1] if points else None
        signal_timestamp = (
            last_point.timestamp if isinstance(last_point, SpreadDataPoint) else simulator.get_last_timestamp()
        )

        # Flush remaining events for reporting purposes.
        events_raw = simulator.consume_events()
        event_dicts = [
            StrategyMetricsFormatter.event_to_dict(event)
            for event in events_raw[-PAYLOAD_EVENT_LIMIT:]
        ]
        trade_dicts = [
            StrategyMetricsFormatter.trade_to_dict(trade)
            for trade in result.trades[-PAYLOAD_TRADE_LIMIT:]
        ]

        payload = StrategyMetricsFormatter.build_payload(
            agent_id=agent_id,
            instrument=f"{parsed.aster_ticker}/{parsed.lighter_symbol}",
            config=config,
            poll_interval=parsed.poll_interval,
            summary=summary,
            open_position=open_position,
            latest_signal=latest_signal,
            signal_timestamp=signal_timestamp,
            events=event_dicts,
            trades=trade_dicts,
            latest_point=last_point,
            mode="simulation",
        )
        _post_metrics_to_coordinator(
            coordinator_url=coordinator_url,
            payload=payload,
            agent_id=agent_id,
        )

    return result


if __name__ == "__main__":  # pragma: no cover - manual invocation
    run_cli()
