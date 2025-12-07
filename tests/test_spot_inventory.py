import asyncio
import sys
from decimal import Decimal
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

from exchanges.base import OrderInfo, OrderResult

if "edgex_sdk" not in sys.modules:
    stub_module = ModuleType("edgex_sdk")
    stub_module.Client = object  # type: ignore[attr-defined]
    stub_module.GetOrderBookDepthParams = object  # type: ignore[attr-defined]
    sys.modules["edgex_sdk"] = stub_module

from strategies.aster_lighter_cycle import CycleConfig, HedgingCycleExecutor


def _spot_config(**overrides) -> CycleConfig:
    config = CycleConfig(
        aster_ticker="ETH",
        lighter_ticker="ETH-PERP",
        quantity=Decimal("1"),
        aster_quantity=Decimal("1"),
        lighter_quantity=Decimal("1"),
        direction="buy",
        take_profit_pct=Decimal("0"),
        slippage_pct=Decimal("0.1"),
        max_wait_seconds=1.0,
        lighter_max_wait_seconds=1.0,
        poll_interval=0.1,
        max_retries=1,
        retry_delay_seconds=0.1,
        max_cycles=1,
        delay_between_cycles=0.0,
        virtual_aster_maker=False,
        lighter_market_type="spot",
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


class _StubLighterClient:
    def __init__(self) -> None:
        self.position = Decimal("0")
        self.order_calls = []
        self.config = SimpleNamespace(contract_id="123", tick_size=Decimal("0.01"))

    async def get_account_positions(self) -> Decimal:
        return self.position

    async def fetch_bbo_prices(self, contract_id: str):
        return Decimal("100"), Decimal("101")

    async def place_limit_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        self.order_calls.append((contract_id, quantity, price, side))
        return OrderResult(success=True, order_id="99", status="PLACED")


def test_spot_mode_auto_preserves_inventory() -> None:
    config = _spot_config(preserve_initial_position=False)

    executor = HedgingCycleExecutor(config)

    assert executor._preserve_initial_lighter_position is True
    assert executor._auto_preserve_spot_inventory is True


def test_ensure_lighter_flat_skips_within_tolerance() -> None:
    config = _spot_config()
    executor = HedgingCycleExecutor(config)
    client = _StubLighterClient()
    executor.lighter_client = cast(Any, client)
    executor.lighter_config.contract_id = "123"
    executor.lighter_config.tick_size = Decimal("0.01")
    executor._lighter_quantity_step = Decimal("0.001")
    executor._baseline_lighter_position = Decimal("5")

    client.position = Decimal("5.0005")

    asyncio.run(executor.ensure_lighter_flat())
    assert client.order_calls == []


def test_ensure_lighter_flat_quantizes_quantity() -> None:
    config = _spot_config()
    executor = HedgingCycleExecutor(config)
    client = _StubLighterClient()
    executor.lighter_client = cast(Any, client)
    executor.lighter_config.contract_id = "123"
    executor.lighter_config.tick_size = Decimal("0.01")
    executor._lighter_quantity_step = Decimal("0.1")
    executor._baseline_lighter_position = Decimal("5")

    client.position = Decimal("5.34")

    mock_fill = OrderInfo(
        order_id="42",
        side="sell",
        size=Decimal("0.3"),
        price=Decimal("100"),
        status="FILLED",
        filled_size=Decimal("0.3"),
    )
    executor._wait_for_lighter_fill = AsyncMock(return_value=mock_fill)  # type: ignore[assignment]

    asyncio.run(executor.ensure_lighter_flat())

    assert client.order_calls, "Expected emergency order to be placed"
    _, quantity, _, _ = client.order_calls[-1]
    assert quantity == Decimal("0.3")
    executor._wait_for_lighter_fill.assert_awaited_once()
