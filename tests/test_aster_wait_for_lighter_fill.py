import asyncio
import sys
import types
from decimal import Decimal
from typing import Any, cast

import pytest


if "edgex_sdk" not in sys.modules:
    class _StubEdgeXClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - test shim
            pass

    class _StubEdgeXParams:  # pragma: no cover - test shim
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    edgex_module = types.ModuleType("edgex_sdk")
    setattr(edgex_module, "Client", _StubEdgeXClient)
    setattr(edgex_module, "GetOrderBookDepthParams", _StubEdgeXParams)
    sys.modules["edgex_sdk"] = edgex_module

from strategies.aster_lighter_cycle import (
    CycleConfig,
    HedgingCycleExecutor,
    SkipCycleError,
)


class _DummyOrder:
    def __init__(self, price: Decimal, side: str):
        self.price = price
        self.side = side


class _DummyLighterClient:
    def __init__(self, position_after: Decimal):
        self.position_after = position_after
        self.current_order = _DummyOrder(price=Decimal("100"), side="buy")
        self.current_order_client_id = 123

    async def get_account_positions(self) -> Decimal:
        return self.position_after


def _make_executor(position_after: Decimal) -> HedgingCycleExecutor:
    config = CycleConfig(
        aster_ticker="ETH",
        lighter_ticker="ETH-PERP",
        quantity=Decimal("1"),
        aster_quantity=Decimal("1"),
        lighter_quantity=Decimal("1"),
        direction="buy",
        take_profit_pct=Decimal("0"),
        slippage_pct=Decimal("0.1"),
        max_wait_seconds=0.0,
        lighter_max_wait_seconds=0.0,
        poll_interval=0.0,
        max_retries=1,
        retry_delay_seconds=0.0,
        max_cycles=1,
        delay_between_cycles=0.0,
        virtual_aster_maker=False,
    )
    executor = HedgingCycleExecutor(config)
    executor.lighter_client = cast(Any, _DummyLighterClient(position_after))
    return executor


def test_wait_for_lighter_fill_assumes_success_when_position_matches():
    executor = _make_executor(position_after=Decimal("1"))

    result = asyncio.run(
        executor._wait_for_lighter_fill(
            "123",
            "LEG2",
            expected_final_position=Decimal("1"),
            expected_fill_size=Decimal("1"),
            expected_side="buy",
            position_before=Decimal("0"),
        )
    )

    assert result.status == "FILLED"
    assert result.filled_size == Decimal("1")


def test_wait_for_lighter_fill_raises_skip_when_position_mismatch():
    executor = _make_executor(position_after=Decimal("0"))

    with pytest.raises(SkipCycleError):
        asyncio.run(
            executor._wait_for_lighter_fill(
                "124",
                "LEG2",
                expected_fill_size=Decimal("1"),
                expected_side="buy",
            )
        )


def test_wait_for_lighter_fill_delta_based_success_without_expected_final():
    # No expected_final_position provided; we only expect a +1 buy movement.
    executor = _make_executor(position_after=Decimal("1"))

    result = asyncio.run(
        executor._wait_for_lighter_fill(
            "125",
            "LEG2",
            expected_fill_size=Decimal("1"),
            expected_side="buy",
        )
    )

    assert result.status == "FILLED"
    assert result.filled_size == Decimal("1")


def test_wait_for_lighter_fill_sell_delta_with_position_before():
    executor = _make_executor(position_after=Decimal("0.5"))

    result = asyncio.run(
        executor._wait_for_lighter_fill(
            "126",
            "LEG2",
            expected_fill_size=Decimal("0.1"),
            expected_side="sell",
            position_before=Decimal("0.6"),
        )
    )

    assert result.status == "FILLED"
    assert result.filled_size == Decimal("0.1")
