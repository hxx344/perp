from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from trading_bot import TradingBot, TradingConfig


def _config(**overrides: object) -> TradingConfig:
    values = {
        "ticker": "BTC",
        "contract_id": "1",
        "quantity": Decimal("0.1"),
        "take_profit": Decimal("0.2"),
        "tick_size": Decimal("0.01"),
        "direction": "buy",
        "max_orders": 5,
        "wait_time": 1,
        "exchange": "lighter",
        "grid_step": Decimal("0.1"),
        "stop_price": Decimal("0"),
        "pause_price": Decimal("0"),
        "boost_mode": False,
    }
    values.update(overrides)
    return TradingConfig(**values)


def _bot(config: TradingConfig | None = None) -> TradingBot:
    bot = object.__new__(TradingBot)
    bot.config = config or _config()
    return bot


def test_config_keeps_maker_depth_before_optional_coordinator_fields() -> None:
    config = _config(coordinator_url="https://coordinator.example", coordinator_vps_id="agent-1")

    assert config.maker_depth_level == 10
    assert config.coordinator_url == "https://coordinator.example"
    assert config.coordinator_vps_id == "agent-1"


def test_close_quantity_accumulates_and_quantizes_residual_fills() -> None:
    bot = _bot()
    bot.exchange_client = SimpleNamespace(quantity_step=Decimal("0.1"))
    bot._residual_close_amount = Decimal("0")

    assert bot._prepare_close_quantity(Decimal("0.04")) is None
    assert bot._residual_close_amount == Decimal("0.04")

    assert bot._prepare_close_quantity(Decimal("0.07")) == Decimal("0.1")
    assert bot._residual_close_amount == Decimal("0.01")

    assert bot._prepare_close_quantity(Decimal("0.24")) == Decimal("0.2")
    assert bot._residual_close_amount == Decimal("0.05")


def test_close_quantity_uses_base_amount_multiplier_as_step_fallback() -> None:
    bot = _bot()
    bot.exchange_client = SimpleNamespace(quantity_step=None, base_amount_multiplier=1000)

    assert bot._quantize_close_amount(Decimal("0.0019")) == Decimal("0.001")


def test_prepare_close_orders_filters_entries_and_uses_decimal_sizes() -> None:
    bot = _bot()
    orders = [
        {
            "id": "entry",
            "side": "sell",
            "size": "9",
            "price": "100",
            "order_type": "OPEN",
        },
        {
            "id": "not-reducing",
            "side": "sell",
            "size": "8",
            "price": "101",
            "reduce_only": "false",
        },
        {
            "id": "close",
            "side": "sell",
            "size": "0.25",
            "price": "102",
            "reduce_only": "true",
        },
    ]

    close_orders, total, close_side = bot._prepare_close_orders(orders, Decimal("0.25"))

    assert close_side == "sell"
    assert total == Decimal("0.25")
    assert close_orders == [
        {
            "id": "close",
            "price": "102",
            "size": Decimal("0.25"),
            "side": "sell",
        }
    ]


def test_signed_position_direction_takes_priority_over_stale_order_hint() -> None:
    bot = _bot()
    bot.exchange_client = SimpleNamespace(positions_are_signed=True)

    assert bot._position_direction_from_amount(Decimal("1")) == "long"
    assert bot._position_direction_from_amount(Decimal("-1")) == "short"
    assert bot._position_direction_from_amount(Decimal("1"), close_side_hint="buy") == "long"
    assert bot._closing_side_for_position(Decimal("-1"), orders=[]) == "buy"


def test_unsigned_position_uses_strategy_direction_for_safe_close_side() -> None:
    bot = _bot(_config(direction="sell"))
    bot.exchange_client = SimpleNamespace(positions_are_signed=False)

    assert bot._position_direction_from_amount(Decimal("1")) == "short"
    assert bot._closing_side_for_position(Decimal("1"), orders=[]) == "buy"


def test_prepare_close_orders_uses_remaining_quantity_after_partial_fill() -> None:
    bot = _bot()
    bot.exchange_client = SimpleNamespace(positions_are_signed=True)
    orders = [
        SimpleNamespace(
            order_id="partial",
            side="sell",
            size=Decimal("0.25"),
            filled_size=Decimal("0.10"),
            remaining_size=Decimal("0.15"),
            price=Decimal("102"),
            reduce_only=True,
        )
    ]

    close_orders, total, close_side = bot._prepare_close_orders(orders, Decimal("0.25"))

    assert close_side == "sell"
    assert total == Decimal("0.15")
    assert close_orders[0]["size"] == Decimal("0.15")


@pytest.mark.asyncio
async def test_mismatch_alerts_are_emitted_only_for_state_transitions() -> None:
    bot = _bot(_config(coordinator_vps_id="agent-1"))
    bot.coordinator_enabled = True
    bot._last_mismatch_alert_state = None
    bot._report_mismatch_alert = AsyncMock()

    values = {
        "position": Decimal("1"),
        "active_close": Decimal("0.5"),
        "mismatch": Decimal("0.5"),
    }
    await bot._update_mismatch_alert_state(severity="warning", **values)
    await bot._update_mismatch_alert_state(severity="warning", **values)
    await bot._update_mismatch_alert_state(severity="critical", **values)
    await bot._update_mismatch_alert_state(severity=None, **values)

    assert [call.kwargs["severity"] for call in bot._report_mismatch_alert.await_args_list] == [
        "warning",
        "critical",
        "resolved",
    ]
    assert bot._last_mismatch_alert_state is None


@pytest.mark.asyncio
async def test_manual_balance_uses_filtered_close_amount_and_refreshes_status() -> None:
    bot = _bot(_config(coordinator_vps_id="agent-1"))
    bot._manual_balance_lock = asyncio.Lock()
    bot.exchange_client = SimpleNamespace(
        get_active_orders=AsyncMock(
            return_value=[
                {
                    "id": "close",
                    "side": "sell",
                    "size": "0.2",
                    "price": "101",
                    "reduce_only": True,
                }
            ]
        ),
        get_account_positions=AsyncMock(return_value=Decimal("0.2")),
    )
    bot.logger = SimpleNamespace(log=lambda *_args, **_kwargs: None)
    bot.active_close_orders = []
    bot._active_close_amount = Decimal("0")
    bot._attempt_auto_balance = AsyncMock(return_value=False)
    bot._update_mismatch_alert_state = AsyncMock()
    bot._mismatch_notified_state = "warning"
    bot.last_log_time = 123
    bot.shutdown_requested = False

    await bot._perform_manual_balance({"reason": "operator request"})

    bot._attempt_auto_balance.assert_awaited_once_with(
        Decimal("0.2"),
        Decimal("0.2"),
        close_side_hint="sell",
    )
    bot._update_mismatch_alert_state.assert_awaited_once_with(
        severity=None,
        position=Decimal("0.2"),
        active_close=Decimal("0.2"),
        mismatch=Decimal("0.0"),
    )
    assert bot._active_close_amount == Decimal("0.2")
    assert bot._mismatch_notified_state is None
    assert bot.last_log_time == 0


@pytest.mark.asyncio
async def test_manual_balance_is_ignored_after_shutdown_starts() -> None:
    bot = _bot(_config(coordinator_vps_id="agent-1"))
    bot._manual_balance_lock = asyncio.Lock()
    bot.shutdown_requested = True
    bot.exchange_client = SimpleNamespace(
        get_active_orders=AsyncMock(),
        get_account_positions=AsyncMock(),
    )

    await bot._perform_manual_balance({"reason": "late command"})

    bot.exchange_client.get_active_orders.assert_not_awaited()
    bot.exchange_client.get_account_positions.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_cancels_inflight_coordinator_actions() -> None:
    bot = _bot(_config(coordinator_vps_id="agent-1"))
    bot._coordinator_tasks = []
    bot._coordinator_action_tasks = set()
    bot._coordinator_session = None
    bot._coordinator_auth = object()
    bot._coordinator_registered = True
    bot.coordinator_enabled = True
    started = asyncio.Event()

    async def pending_action() -> None:
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(pending_action())
    bot._coordinator_action_tasks.add(task)
    await started.wait()

    await bot._shutdown_coordinator()

    assert task.cancelled()
    assert bot._coordinator_action_tasks == set()
    assert bot._coordinator_registered is False


@pytest.mark.asyncio
async def test_filled_order_closes_actual_quantized_fill() -> None:
    bot = _bot(_config(exchange="aster"))
    bot.exchange_client = SimpleNamespace(
        quantity_step=Decimal("0.1"),
        current_order=None,
        place_close_order=AsyncMock(
            return_value=SimpleNamespace(success=True, error_message=None)
        ),
    )
    bot.logger = SimpleNamespace(log=lambda *_args, **_kwargs: None)
    bot.order_filled_event = asyncio.Event()
    bot.order_filled_event.set()
    bot.order_filled_amount = Decimal("0.15")
    bot._residual_close_amount = Decimal("0")
    bot.last_open_order_time = 0
    order_result = SimpleNamespace(
        order_id="open-1",
        price=Decimal("100"),
        status="FILLED",
        size=Decimal("0.1"),
        filled_size=Decimal("0.15"),
    )

    result = await bot._handle_order_result(order_result)

    assert result is True
    bot.exchange_client.place_close_order.assert_awaited_once_with(
        "1",
        Decimal("0.1"),
        Decimal("100.2"),
        "sell",
    )
    assert bot._residual_close_amount == Decimal("0.05")


@pytest.mark.asyncio
async def test_coordinator_metrics_reuse_account_snapshot() -> None:
    bot = _bot(_config(coordinator_vps_id="agent-1"))
    bot.coordinator_enabled = True
    bot._coordinator_trade_volume = Decimal("50")
    bot._active_close_amount = Decimal("0")
    bot._residual_close_amount = Decimal("0")
    bot.logger = SimpleNamespace(log=lambda *_args, **_kwargs: None)
    bot.exchange_client = SimpleNamespace(
        positions_are_signed=True,
        get_account_metrics=AsyncMock(
            return_value={
                "position_size": Decimal("-0.2"),
                "position_symbol": "BTC",
                "position_value": Decimal("13000"),
                "available_balance": Decimal("100"),
                "total_account_value": Decimal("500"),
            }
        ),
        get_account_positions=AsyncMock(return_value=Decimal("99")),
        get_position_snapshot=AsyncMock(return_value={"size": Decimal("99")}),
        get_active_orders=AsyncMock(return_value=[]),
        get_available_balance=AsyncMock(return_value=Decimal("999")),
    )

    payload = await bot._build_coordinator_metrics_payload()

    assert payload is not None
    assert payload["position"] == "-0.2"
    assert payload["position_direction"] == "short"
    assert payload["position_symbol"] == "BTC"
    assert payload["position_value"] == "13000"
    assert payload["balance"] == "100"
    bot.exchange_client.get_account_positions.assert_not_awaited()
    bot.exchange_client.get_position_snapshot.assert_not_awaited()
    bot.exchange_client.get_available_balance.assert_not_awaited()
