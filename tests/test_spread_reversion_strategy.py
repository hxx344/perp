from decimal import Decimal
from math import sin

import pytest

from strategies.spread_reversion_strategy import (
    StrategyConfig,
    SpreadDataPoint,
    SpreadReversionSimulator,
    run_cli,
)


def _point(ts: float, aster_mid: Decimal, lighter_mid: Decimal) -> SpreadDataPoint:
    half_spread_aster = Decimal("0.05")
    half_spread_lighter = Decimal("0.04")
    return SpreadDataPoint(
        timestamp=float(ts),
        aster_bid=aster_mid - half_spread_aster,
        aster_ask=aster_mid + half_spread_aster,
        lighter_bid=lighter_mid - half_spread_lighter,
        lighter_ask=lighter_mid + half_spread_lighter,
    )


def _build_dataset() -> list[SpreadDataPoint]:
    points: list[SpreadDataPoint] = []
    base = Decimal("100")

    # Warm-up regime with small oscillations
    for idx in range(60):
        spread = Decimal(str(0.2 * sin(idx / 6)))
        lighter_mid = base + Decimal(idx) * Decimal("0.01")
        aster_mid = lighter_mid + spread
        points.append(_point(idx, aster_mid, lighter_mid))

    # Positive dislocation to trigger short-aster / long-lighter entry
    for idx in range(60, 80):
        lighter_mid = base + Decimal(idx) * Decimal("0.01")
        aster_mid = lighter_mid + Decimal("3.0") - Decimal(idx - 60) * Decimal("0.05")
        points.append(_point(idx, aster_mid, lighter_mid))

    # Reversion towards neutral spread
    for idx in range(80, 110):
        lighter_mid = base + Decimal(idx) * Decimal("0.01")
        aster_mid = lighter_mid + Decimal("0.1") * Decimal(idx - 90) / Decimal("10")
        points.append(_point(idx, aster_mid, lighter_mid))

    # Negative dislocation for the opposite trade
    for idx in range(110, 130):
        lighter_mid = base + Decimal(idx) * Decimal("0.01")
        aster_mid = lighter_mid - Decimal("3.2") + Decimal(idx - 110) * Decimal("0.06")
        points.append(_point(idx, aster_mid, lighter_mid))

    # Revert to neutral again
    for idx in range(130, 160):
        lighter_mid = base + Decimal(idx) * Decimal("0.01")
        aster_mid = lighter_mid - Decimal("0.1") * Decimal(idx - 145) / Decimal("10")
        points.append(_point(idx, aster_mid, lighter_mid))

    return points


def test_simulator_executes_spread_trades():
    config = StrategyConfig(
        rolling_window=40,
        enter_z=Decimal("2.2"),
        exit_z=Decimal("0.6"),
        stop_z=Decimal("4.0"),
        quantity=Decimal("2"),
        max_holding_ticks=60,
        min_abs_spread=Decimal("0.5"),
        aster_fee_rate=Decimal("0"),
        lighter_fee_rate=Decimal("0"),
    )
    simulator = SpreadReversionSimulator(config)
    result = simulator.process(_build_dataset())

    assert result.trade_count >= 2
    assert result.total_pnl > 0

    directions = [trade.direction for trade in result.trades]
    assert "short_aster_long_lighter" in directions
    assert "long_aster_short_lighter" in directions


@pytest.mark.parametrize("args", [["--demo"], ["--demo", "--enter-z", "1.5", "--exit-z", "0.4"]])
def test_cli_returns_simulation_result(args, capsys):
    result = run_cli(args)
    captured = capsys.readouterr()
    assert "Spread Reversion Test Result" in captured.out
    assert result.trade_count >= 1


def test_lighter_only_strategy_generates_single_leg_trade():
    config = StrategyConfig(
        rolling_window=10,
        enter_z=Decimal("0.5"),
        exit_z=Decimal("0.1"),
        stop_z=Decimal("4"),
        quantity=Decimal("1"),
        max_holding_ticks=10,
        min_abs_spread=Decimal("0.1"),
        aster_fee_rate=Decimal("0"),
        lighter_fee_rate=Decimal("0"),
        lighter_only=True,
    )

    points: list[SpreadDataPoint] = []
    base_aster = Decimal("100")
    base_lighter = Decimal("100")
    for idx in range(10):
        aster_mid = base_aster + Decimal(idx) * Decimal("0.1")
        lighter_mid = base_lighter + Decimal(idx) * Decimal("0.1") - Decimal("0.1")
        points.append(_point(idx, aster_mid, lighter_mid))

    points.extend(
        [
            _point(10, Decimal("103"), Decimal("100")),  # large positive spread triggers entry
            _point(11, Decimal("101.2"), Decimal("101.1")),
            _point(12, Decimal("101"), Decimal("101")),
        ]
    )

    simulator = SpreadReversionSimulator(config)
    result = simulator.process(points)

    assert result.trade_count == 1
    assert result.total_pnl > 0
    assert result.trades[0].direction == "lighter_long"
