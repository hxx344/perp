from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from strategies.neutral_dashboard import NeutralAction
from strategies.rh_neutral_manager import AccountSpec, NeutralPositionManager, NeutralSettings


def _settings() -> NeutralSettings:
    return NeutralSettings(
        main=AccountSpec("main", 10, 4),
        sub=AccountSpec("sub", 11, 4),
        spy_market_id=101,
        qqq_market_id=102,
    )


@pytest.mark.asyncio
async def test_dashboard_dispatch_maps_single_position_and_pair_actions():
    manager = NeutralPositionManager(_settings())
    manager.close_one = AsyncMock(return_value={"status": "ok"})
    manager.close_both = AsyncMock(return_value={"status": "ok"})
    manager.gateways = {"main": object(), "sub": object()}

    single = NeutralAction(
        action="close_position",
        request_id="single-1",
        account="main",
        symbol="SPY",
        quantity=Decimal("0.25"),
    )
    assert await manager._handle_dashboard_action(single) == {"status": "ok"}
    manager.close_one.assert_awaited_once_with("main", "SPY", Decimal("0.25"), request_id="single-1")

    pair = NeutralAction(
        action="close_pair",
        request_id="pair-1",
        account=None,
        symbol="QQQ",
        quantity=Decimal("0.1"),
    )
    assert await manager._handle_dashboard_action(pair) == {"status": "ok"}
    manager.close_both.assert_awaited_once_with(
        "QQQ",
        {"main": Decimal("0.1"), "sub": Decimal("0.1")},
        request_id="pair-1",
    )


@pytest.mark.asyncio
async def test_dashboard_dispatch_maps_rebalance_and_flatten_all():
    manager = NeutralPositionManager(_settings())
    manager.manual_rebalance = AsyncMock(return_value={"status": "balanced"})
    manager.flatten_all = AsyncMock(return_value={"status": "flat"})

    assert await manager._handle_dashboard_action(
        NeutralAction(action="rebalance", request_id="rebalance-1")
    ) == {"status": "balanced"}
    manager.manual_rebalance.assert_awaited_once_with(request_id="rebalance-1")

    assert await manager._handle_dashboard_action(
        NeutralAction(action="flatten_all", request_id="flatten-1")
    ) == {"status": "flat"}
    manager.flatten_all.assert_awaited_once_with(request_id="flatten-1")
