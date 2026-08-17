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
        self.order_options = []
        self.config = SimpleNamespace(contract_id="123", tick_size=Decimal("0.01"))

    async def get_account_positions(self) -> Decimal:
        return self.position

    async def fetch_bbo_prices(self, contract_id: str):
        return Decimal("100"), Decimal("101")

    async def place_limit_order(
        self,
        contract_id: str,
        quantity: Decimal,
        price: Decimal,
        side: str,
        **kwargs,
    ) -> OrderResult:
        self.order_calls.append((contract_id, quantity, price, side))
        self.order_options.append(kwargs)
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


def test_ensure_lighter_flat_waits_for_stale_position_snapshot() -> None:
    config = _spot_config()
    config.lighter_position_settle_seconds = 0.2
    executor = HedgingCycleExecutor(config)
    client = _StubLighterClient()
    client.current_order = OrderInfo(
        order_id="43",
        side="buy",
        size=Decimal("0.3"),
        price=Decimal("100"),
        status="FILLED",
        filled_size=Decimal("0.3"),
    )
    client.get_account_positions = AsyncMock(
        side_effect=[Decimal("-0.00213"), Decimal("0")]
    )
    executor.lighter_client = cast(Any, client)
    executor.lighter_config.contract_id = "123"
    executor.lighter_config.tick_size = Decimal("0.01")
    executor._lighter_quantity_step = Decimal("0.001")
    executor._baseline_lighter_position = Decimal("0")

    asyncio.run(executor.ensure_lighter_flat())

    assert client.order_calls == []
    assert client.get_account_positions.await_count == 2


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
    assert client.order_options[-1] == {"time_in_force": "ioc", "reduce_only": True}
    executor._wait_for_lighter_fill.assert_awaited_once()


def test_ensure_lighter_flat_retries_ioc_partial_fill() -> None:
    config = _spot_config()
    executor = HedgingCycleExecutor(config)
    client = _StubLighterClient()
    executor.lighter_client = cast(Any, client)
    executor.lighter_config.contract_id = "123"
    executor.lighter_config.tick_size = Decimal("0.01")
    executor._lighter_quantity_step = Decimal("0.1")
    executor._baseline_lighter_position = Decimal("5")

    client.get_account_positions = AsyncMock(
        side_effect=[Decimal("5.3"), Decimal("5.2"), Decimal("5.2")]
    )
    completed_fill = OrderInfo(
        order_id="43",
        side="sell",
        size=Decimal("0.2"),
        price=Decimal("100"),
        status="FILLED",
        filled_size=Decimal("0.2"),
    )
    executor._wait_for_lighter_fill = AsyncMock(
        side_effect=[RuntimeError("CANCELED_PARTIAL"), completed_fill]
    )  # type: ignore[assignment]

    asyncio.run(executor.ensure_lighter_flat())

    assert len(client.order_calls) == 2
    assert client.order_calls[0][1] == Decimal("0.3")
    assert client.order_calls[1][1] == Decimal("0.2")
    assert executor._wait_for_lighter_fill.await_count == 2
    assert executor._lighter_recovery_blocked is False


def test_ensure_lighter_flat_reports_residual_below_market_minimum() -> None:
    config = _spot_config()
    executor = HedgingCycleExecutor(config)
    client = _StubLighterClient()
    client.min_base_amount = Decimal("0.2")
    client.min_quote_amount = Decimal("10")
    executor.lighter_client = cast(Any, client)
    executor.lighter_config.contract_id = "123"
    executor.lighter_config.tick_size = Decimal("0.01")
    executor._lighter_quantity_step = Decimal("0.01")
    executor._baseline_lighter_position = Decimal("5")

    client.position = Decimal("5.1")
    executor._wait_for_lighter_fill = AsyncMock(
        side_effect=RuntimeError("CANCELED_PARTIAL")
    )  # type: ignore[assignment]

    asyncio.run(executor.ensure_lighter_flat())

    assert len(client.order_calls) == 0
    assert executor._lighter_recovery_blocked is True
