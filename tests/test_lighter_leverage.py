import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

from strategies.aster_lighter_cycle import CycleConfig, HedgingCycleExecutor


class _FakeSignerClient:
    CROSS_MARGIN_MODE = 7

    def __init__(self, *, error=None):
        self.calls = []
        self._error = error

    async def update_leverage(self, market_index: int, margin_mode: int, leverage: int):
        self.calls.append((market_index, margin_mode, leverage))
        return ("0xtx", None, self._error)


class _FakeLighterClient(SimpleNamespace):
    lighter_client: _FakeSignerClient
    config: SimpleNamespace


def _make_executor(signer: _FakeSignerClient) -> HedgingCycleExecutor:
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
    executor.lighter_client = cast(
        Any,
        _FakeLighterClient(
            lighter_client=signer,
            config=SimpleNamespace(contract_id="123"),
        ),
    )
    return executor


def test_ensure_lighter_leverage_invokes_signer_update():
    signer = _FakeSignerClient()
    executor = _make_executor(signer)

    asyncio.run(executor._ensure_lighter_leverage(50))

    assert signer.calls == [(123, signer.CROSS_MARGIN_MODE, 50)]


def test_ensure_lighter_leverage_swallows_already_set_error():
    signer = _FakeSignerClient(error=RuntimeError("leverage already the same"))
    executor = _make_executor(signer)

    asyncio.run(executor._ensure_lighter_leverage(50))

    assert signer.calls == [(123, signer.CROSS_MARGIN_MODE, 50)]


def test_ensure_lighter_leverage_raises_on_other_error():
    signer = _FakeSignerClient(error=RuntimeError("something else failed"))
    executor = _make_executor(signer)

    with pytest.raises(RuntimeError):
        asyncio.run(executor._ensure_lighter_leverage(25))

    assert signer.calls == [(123, signer.CROSS_MARGIN_MODE, 25)]
