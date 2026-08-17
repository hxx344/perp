import sys
from decimal import Decimal

from strategies.aster_lighter_cycle import CycleConfig, HedgingCycleExecutor, _parse_args


def _config(**overrides):
    values = {
        "aster_ticker": "BTC",
        "lighter_ticker": "BTC",
        "quantity": Decimal("0.001"),
        "aster_quantity": Decimal("0.01"),
        "lighter_quantity": Decimal("0.02"),
        "direction": "buy",
        "take_profit_pct": Decimal("0"),
        "slippage_pct": Decimal("0.3"),
        "max_wait_seconds": 3.0,
        "lighter_max_wait_seconds": 10.0,
        "poll_interval": 0.1,
        "max_retries": 1,
        "retry_delay_seconds": 0.0,
        "max_cycles": 1,
        "delay_between_cycles": 0.0,
        "virtual_aster_maker": True,
        "lighter_quantity_min": Decimal("0.0104"),
        "lighter_quantity_max": Decimal("0.0126"),
        "quantity_seed": 19,
    }
    values.update(overrides)
    return CycleConfig(**values)


def test_random_lighter_quantity_is_bounded_and_step_aligned():
    executor = HedgingCycleExecutor(_config())
    executor._lighter_quantity_step = Decimal("0.001")

    values = [executor._select_lighter_order_quantity() for _ in range(40)]

    assert set(values).issubset({Decimal("0.011"), Decimal("0.012")})
    assert all(
        executor._lighter_quantity_min <= value <= executor._lighter_quantity_max
        for value in values
    )


def test_quantity_seed_is_reproducible_and_cycle_quantity_is_shared():
    first = HedgingCycleExecutor(_config(quantity_seed=7))
    second = HedgingCycleExecutor(_config(quantity_seed=7))
    first._lighter_quantity_step = Decimal("0.001")
    second._lighter_quantity_step = Decimal("0.001")

    first_values = [first._select_lighter_order_quantity() for _ in range(8)]
    second_values = [second._select_lighter_order_quantity() for _ in range(8)]
    assert first_values == second_values

    first._current_cycle_lighter_quantity = None
    selected = first._prepare_cycle_quantity()
    assert first._prepare_cycle_quantity() == selected


def test_cycle_quantity_rejects_other_venue_constraint_outside_range():
    executor = HedgingCycleExecutor(_config())
    executor._lighter_quantity_step = Decimal("0.001")
    executor._aster_min_quantity = Decimal("0.013")

    try:
        executor._prepare_cycle_quantity()
    except ValueError as exc:
        assert "No executable cycle quantity" in str(exc)
    else:
        raise AssertionError("expected an incompatible venue constraint to fail closed")


def test_cli_exposes_lighter_quantity_range_and_seed(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "aster_lighter_cycle.py",
            "--aster-ticker",
            "BTC",
            "--lighter-ticker",
            "BTC",
            "--quantity",
            "0.001",
            "--lighter-quantity-min",
            "0.01",
            "--lighter-quantity-max",
            "0.02",
            "--lighter-quantity-seed",
            "123",
        ],
    )

    args = _parse_args()

    assert args.lighter_quantity_min == Decimal("0.01")
    assert args.lighter_quantity_max == Decimal("0.02")
    assert args.lighter_quantity_seed == 123
