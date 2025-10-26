import asyncio
import random
from decimal import Decimal
import types
from typing import Any, Dict, List, Tuple, cast

from strategies.aster_lighter_cycle import CycleConfig, HedgingCycleExecutor, LegResult


def _build_config(**overrides: Any) -> CycleConfig:
    base_kwargs: Dict[str, Any] = {
        "aster_ticker": "ETH",
        "lighter_ticker": "ETH-PERP",
        "quantity": Decimal("1"),
        "aster_quantity": Decimal("1"),
        "lighter_quantity": Decimal("1"),
        "direction": "buy",
        "take_profit_pct": Decimal("0"),
        "slippage_pct": Decimal("0.1"),
        "max_wait_seconds": 0.0,
        "lighter_max_wait_seconds": 0.0,
        "poll_interval": 0.0,
        "max_retries": 1,
        "retry_delay_seconds": 0.0,
        "max_cycles": 1,
        "delay_between_cycles": 0.0,
        "virtual_aster_maker": False,
    }
    base_kwargs.update(overrides)
    return CycleConfig(**base_kwargs)


def _attach_leg_stubs(executor: HedgingCycleExecutor, recorder: List[Tuple[str, str]]) -> None:
    async def stub_aster_maker(self: HedgingCycleExecutor, leg_name: str, direction: str) -> LegResult:
        recorder.append((leg_name, direction))
        return LegResult(
            name=leg_name,
            exchange="aster",
            side=direction,
            quantity=Decimal("1"),
            price=Decimal("100"),
            order_id=f"{leg_name}-order",
            status="FILLED",
            latency_seconds=0.0,
        )

    async def stub_aster_reverse(self: HedgingCycleExecutor, leg_name: str, direction: str) -> LegResult:
        recorder.append((leg_name, direction))
        return LegResult(
            name=leg_name,
            exchange="aster",
            side=direction,
            quantity=Decimal("1"),
            price=Decimal("101"),
            order_id=f"{leg_name}-order",
            status="FILLED",
            latency_seconds=0.0,
        )

    async def stub_lighter(
        self: HedgingCycleExecutor,
        leg_name: str,
        direction: str,
        reference_price: Decimal | None = None,
    ) -> LegResult:
        recorder.append((leg_name, direction))
        return LegResult(
            name=leg_name,
            exchange="lighter",
            side=direction,
            quantity=Decimal("1"),
            price=Decimal("102"),
            order_id=f"{leg_name}-order",
            status="FILLED",
            latency_seconds=0.0,
            reference_price=reference_price,
        )

    executor._execute_aster_maker = types.MethodType(stub_aster_maker, executor)
    executor._execute_aster_reverse_maker = types.MethodType(stub_aster_reverse, executor)
    executor._execute_lighter_taker = types.MethodType(stub_lighter, executor)


def test_execute_cycle_obeys_config_direction_without_randomization() -> None:
    config = _build_config(randomize_direction=False)
    executor = HedgingCycleExecutor(config)
    executor.aster_client = cast(Any, object())
    executor.lighter_client = cast(Any, object())

    recorded: List[Tuple[str, str]] = []
    _attach_leg_stubs(executor, recorded)

    results = asyncio.run(executor.execute_cycle())

    assert [direction for _, direction in recorded] == ["buy", "sell", "sell", "buy"]
    assert executor._current_cycle_entry_direction == "buy"
    assert all(isinstance(result, LegResult) for result in results)


def test_execute_cycle_randomizes_direction_when_enabled() -> None:
    config = _build_config(randomize_direction=True)
    executor = HedgingCycleExecutor(config)
    executor.aster_client = cast(Any, object())
    executor.lighter_client = cast(Any, object())

    class DummyRng(random.Random):
        def __init__(self) -> None:
            super().__init__(0)
            self.calls = 0

        def choice(self, sequence: list[str]) -> str:  # type: ignore[override]
            self.calls += 1
            assert sequence == ["buy", "sell"]
            return "sell"

    dummy_rng = DummyRng()
    executor._direction_rng = dummy_rng

    recorded: List[Tuple[str, str]] = []
    _attach_leg_stubs(executor, recorded)

    results = asyncio.run(executor.execute_cycle())

    assert dummy_rng.calls == 1
    assert [direction for _, direction in recorded] == ["sell", "buy", "buy", "sell"]
    assert executor._current_cycle_entry_direction == "sell"
    assert all(isinstance(result, LegResult) for result in results)
